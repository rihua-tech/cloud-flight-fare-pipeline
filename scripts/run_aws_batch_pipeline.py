"""Batch entrypoint for the Week 9 ECS/Fargate proof path.

The runner intentionally composes existing project commands instead of
duplicating ingestion, Redshift, or dbt logic.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent
from typing import Mapping

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
DBT_PROFILE_NAME = "flight_fares"
DBT_PROJECT_DIR = "dbt/flight_fares"
DBT_PROFILES_DIR_ENV = "BATCH_DBT_PROFILES_DIR"

DEFAULT_STEPS = ("s3_ingest", "redshift_load", "dbt_build", "redshift_verify")
VALID_STEPS = set(DEFAULT_STEPS)


@dataclass(frozen=True)
class BatchConfig:
    steps: tuple[str, ...]
    run_date: str
    start_date: str | None
    days: int
    rerun_behavior: str
    dbt_target: str
    run_dbt_deps: bool


def utc_today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def env_bool(value: str | None, default: bool) -> bool:
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_steps(value: str | None) -> tuple[str, ...]:
    if not value:
        return DEFAULT_STEPS

    steps = tuple(step.strip() for step in value.split(",") if step.strip())
    unknown = [step for step in steps if step not in VALID_STEPS]
    if unknown:
        raise ValueError(
            "Unknown BATCH_PIPELINE_STEPS value(s): "
            + ", ".join(unknown)
            + ". Valid values: "
            + ", ".join(sorted(VALID_STEPS))
        )
    if not steps:
        raise ValueError("BATCH_PIPELINE_STEPS did not contain any valid step names")
    return steps


def build_config(env: Mapping[str, str] | None = None) -> BatchConfig:
    env = os.environ if env is None else env
    days = int(env.get("PIPELINE_DAYS", "1"))
    if days < 1:
        raise ValueError("PIPELINE_DAYS must be at least 1")

    return BatchConfig(
        steps=parse_steps(env.get("BATCH_PIPELINE_STEPS")),
        run_date=env.get("PIPELINE_RUN_DATE") or utc_today(),
        start_date=env.get("PIPELINE_START_DATE") or None,
        days=days,
        rerun_behavior=env.get("BRONZE_RERUN_BEHAVIOR", "overwrite"),
        dbt_target=env.get("BATCH_DBT_TARGET", "redshift"),
        run_dbt_deps=env_bool(env.get("BATCH_DBT_DEPS"), default=True),
    )


def ingest_command(config: BatchConfig) -> list[str]:
    command = [
        sys.executable,
        "-m",
        "ingestion.ingest_api_to_s3",
        "--mode",
        "s3",
        "--rerun-behavior",
        config.rerun_behavior,
    ]
    if config.start_date:
        command.extend(["--start", config.start_date, "--days", str(config.days)])
    else:
        command.extend(["--date", config.run_date])
    return command


def redshift_load_command() -> list[str]:
    return [sys.executable, "warehouse/run_redshift_sql.py"]


def redshift_verify_command() -> list[str]:
    return [sys.executable, "warehouse/run_redshift_sql.py", "--files", "verify_marts.sql"]


def dbt_profiles_dir(env: Mapping[str, str] | None = None) -> Path:
    env = os.environ if env is None else env
    configured_dir = env.get(DBT_PROFILES_DIR_ENV)
    if configured_dir:
        return Path(configured_dir)
    return Path(tempfile.gettempdir()) / "cloud-flight-fare-pipeline" / "dbt-profiles"


def dbt_profiles_yml() -> str:
    return dedent(
        f"""\
        {DBT_PROFILE_NAME}:
          target: "{{{{ env_var('BATCH_DBT_TARGET', 'redshift') }}}}"
          outputs:
            redshift:
              type: redshift
              host: "{{{{ env_var('REDSHIFT_HOST') }}}}"
              port: "{{{{ env_var('REDSHIFT_PORT', '5439') | int }}}}"
              user: "{{{{ env_var('REDSHIFT_USER') }}}}"
              password: "{{{{ env_var('REDSHIFT_PASSWORD') }}}}"
              dbname: "{{{{ env_var('REDSHIFT_DBNAME', 'dev') }}}}"
              schema: "{{{{ env_var('REDSHIFT_SCHEMA_STAGING', 'staging') }}}}"
              threads: "{{{{ env_var('DBT_THREADS', '4') | int }}}}"
              sslmode: "{{{{ env_var('REDSHIFT_SSLMODE', 'require') }}}}"
        """
    )


def write_dbt_profiles_file(profiles_dir: Path | None = None) -> Path:
    profiles_dir = dbt_profiles_dir() if profiles_dir is None else profiles_dir
    profiles_dir.mkdir(parents=True, exist_ok=True)
    profiles_path = profiles_dir / "profiles.yml"
    profiles_path.write_text(dbt_profiles_yml(), encoding="utf-8")
    return profiles_path


def prepare_dbt_profiles() -> Path:
    profiles_path = write_dbt_profiles_file()
    print(
        "WEEK9_DBT_PROFILE_GENERATED "
        f"path={profiles_path} "
        f"profile={DBT_PROFILE_NAME} "
        f"sslmode={os.getenv('REDSHIFT_SSLMODE', 'require')} "
        f"staging_schema={os.getenv('REDSHIFT_SCHEMA_STAGING', 'staging')} "
        f"marts_schema={os.getenv('REDSHIFT_SCHEMA_MARTS', 'marts')}",
        flush=True,
    )
    return profiles_path.parent


def dbt_commands(config: BatchConfig, profiles_dir: Path | None = None) -> list[list[str]]:
    commands: list[list[str]] = []
    profiles_dir_arg = str(dbt_profiles_dir() if profiles_dir is None else profiles_dir)
    if config.run_dbt_deps:
        commands.append(
            [
                "dbt",
                "deps",
                "--project-dir",
                DBT_PROJECT_DIR,
                "--profiles-dir",
                profiles_dir_arg,
            ]
        )
    commands.append(
        [
            "dbt",
            "build",
            "--project-dir",
            DBT_PROJECT_DIR,
            "--profiles-dir",
            profiles_dir_arg,
            "-t",
            config.dbt_target,
        ]
    )
    return commands


def commands_for_step(step: str, config: BatchConfig) -> list[list[str]]:
    if step == "s3_ingest":
        return [ingest_command(config)]
    if step == "redshift_load":
        return [redshift_load_command()]
    if step == "dbt_build":
        profiles_dir = prepare_dbt_profiles()
        return dbt_commands(config, profiles_dir=profiles_dir)
    if step == "redshift_verify":
        return [redshift_verify_command()]
    raise ValueError(f"Unsupported batch step: {step}")


def command_label(command: list[str]) -> str:
    return " ".join(command)


def print_runtime_summary(config: BatchConfig) -> None:
    print("WEEK9_BATCH_START")
    print(f"repo_root={ROOT}")
    print(f"steps={','.join(config.steps)}")
    print(f"run_date={config.run_date}")
    print(f"start_date={config.start_date or ''}")
    print(f"days={config.days}")
    print(f"dbt_target={config.dbt_target}")
    print(f"aws_region={os.getenv('AWS_REGION', '')}")
    print(f"s3_bucket={os.getenv('S3_BUCKET', '')}")
    print(f"s3_prefix_bronze={os.getenv('S3_PREFIX_BRONZE', '')}")
    print(f"s3_copy_uri={os.getenv('S3_COPY_URI', '')}")
    print(f"redshift_host={os.getenv('REDSHIFT_HOST', '')}")
    print(f"redshift_dbname={os.getenv('REDSHIFT_DBNAME', '')}")
    print(f"redshift_user={os.getenv('REDSHIFT_USER', '')}")
    print(f"redshift_sslmode={os.getenv('REDSHIFT_SSLMODE', 'require')}")
    print(f"redshift_schema_staging={os.getenv('REDSHIFT_SCHEMA_STAGING', 'staging')}")
    print(f"redshift_schema_marts={os.getenv('REDSHIFT_SCHEMA_MARTS', 'marts')}")


def run_command(command: list[str]) -> int:
    print(f"WEEK9_COMMAND_START {command_label(command)}", flush=True)
    completed = subprocess.run(command, cwd=ROOT, check=False)
    if completed.returncode == 0:
        print(f"WEEK9_COMMAND_SUCCESS {command_label(command)}", flush=True)
    else:
        print(
            f"WEEK9_COMMAND_FAILED exit_code={completed.returncode} {command_label(command)}",
            flush=True,
        )
    return completed.returncode


def main() -> int:
    load_dotenv()
    try:
        config = build_config()
    except ValueError as exc:
        print(f"WEEK9_CONFIG_ERROR {exc}", flush=True)
        return 2

    print_runtime_summary(config)
    for step in config.steps:
        print(f"WEEK9_STEP_START {step}", flush=True)
        for command in commands_for_step(step, config):
            return_code = run_command(command)
            if return_code != 0:
                print(f"WEEK9_STEP_FAILED {step}", flush=True)
                return return_code
        print(f"WEEK9_STEP_SUCCESS {step}", flush=True)

    print("WEEK9_BATCH_SUCCESS", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
