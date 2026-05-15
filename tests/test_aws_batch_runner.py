import sys

import pytest

import scripts.run_aws_batch_pipeline as runner


def test_build_config_uses_safe_defaults(monkeypatch):
    monkeypatch.setattr(runner, "utc_today", lambda: "2026-05-14")

    config = runner.build_config({})

    assert config.steps == (
        "s3_ingest",
        "redshift_load",
        "dbt_build",
        "redshift_verify",
    )
    assert config.run_date == "2026-05-14"
    assert config.days == 1
    assert config.dbt_target == "redshift"
    assert config.run_dbt_deps


def test_parse_steps_rejects_unknown_step():
    with pytest.raises(ValueError, match="Unknown BATCH_PIPELINE_STEPS"):
        runner.parse_steps("s3_ingest,not_a_step")


def test_ingest_command_uses_start_date_for_backfills():
    config = runner.BatchConfig(
        steps=("s3_ingest",),
        run_date="2026-05-14",
        start_date="2026-01-17",
        days=3,
        rerun_behavior="skip-existing",
        dbt_target="redshift",
        run_dbt_deps=True,
    )

    command = runner.ingest_command(config)

    assert command == [
        sys.executable,
        "-m",
        "ingestion.ingest_api_to_s3",
        "--mode",
        "s3",
        "--rerun-behavior",
        "skip-existing",
        "--start",
        "2026-01-17",
        "--days",
        "3",
    ]


def test_dbt_commands_can_skip_deps():
    config = runner.BatchConfig(
        steps=("dbt_build",),
        run_date="2026-05-14",
        start_date=None,
        days=1,
        rerun_behavior="overwrite",
        dbt_target="redshift",
        run_dbt_deps=False,
    )

    commands = runner.dbt_commands(config)

    assert len(commands) == 1
    assert commands[0][:2] == ["dbt", "build"]
    assert commands[0][-2:] == ["-t", "redshift"]


def test_dbt_commands_use_runtime_profiles_dir(tmp_path):
    config = runner.BatchConfig(
        steps=("dbt_build",),
        run_date="2026-05-14",
        start_date=None,
        days=1,
        rerun_behavior="overwrite",
        dbt_target="redshift",
        run_dbt_deps=True,
    )

    commands = runner.dbt_commands(config, profiles_dir=tmp_path)

    assert len(commands) == 2
    for command in commands:
        profiles_arg_index = command.index("--profiles-dir") + 1
        assert command[profiles_arg_index] == str(tmp_path)


def test_runtime_dbt_profile_uses_flight_fares_and_env_vars(tmp_path, monkeypatch):
    monkeypatch.setenv("REDSHIFT_PASSWORD", "do-not-write-this-value")

    profiles_path = runner.write_dbt_profiles_file(tmp_path)
    profiles_yml = profiles_path.read_text(encoding="utf-8")

    assert profiles_path == tmp_path / "profiles.yml"
    assert profiles_yml.startswith("flight_fares:")
    assert "password: \"{{ env_var('REDSHIFT_PASSWORD') }}\"" in profiles_yml
    assert "host: \"{{ env_var('REDSHIFT_HOST') }}\"" in profiles_yml
    assert "schema: \"{{ env_var('REDSHIFT_SCHEMA_STAGING', 'staging') }}\"" in profiles_yml
    assert "sslmode: \"{{ env_var('REDSHIFT_SSLMODE', 'require') }}\"" in profiles_yml
    assert "do-not-write-this-value" not in profiles_yml
