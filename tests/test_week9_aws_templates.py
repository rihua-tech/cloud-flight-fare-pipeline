import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
AWS_DIR = ROOT / "aws"


def load_template(name: str) -> dict:
    return json.loads((AWS_DIR / name).read_text(encoding="utf-8"))


def test_week9_json_templates_are_valid():
    for path in AWS_DIR.glob("*.template.json"):
        json.loads(path.read_text(encoding="utf-8"))


def test_ecs_task_definition_uses_cloudwatch_logs_and_secret_password():
    task_definition = load_template("ecs-task-definition.template.json")
    container = task_definition["containerDefinitions"][0]
    env_names = {item["name"] for item in container["environment"]}
    secret_names = {item["name"] for item in container["secrets"]}

    assert task_definition["requiresCompatibilities"] == ["FARGATE"]
    assert container["logConfiguration"]["logDriver"] == "awslogs"
    assert "REDSHIFT_PASSWORD" not in env_names
    assert "REDSHIFT_PASSWORD" in secret_names
    assert "REDSHIFT_SCHEMA_STAGING" in env_names
    assert "REDSHIFT_SCHEMA_MARTS" in env_names
    assert "REDSHIFT_SSLMODE" in env_names


def test_eventbridge_scheduler_template_targets_fargate_task():
    schedule = load_template("eventbridge-scheduler.template.json")
    target = schedule["Target"]
    ecs_parameters = target["EcsParameters"]

    assert target["Arn"].startswith("arn:aws:ecs:<region>:<account-id>:cluster/")
    assert ecs_parameters["LaunchType"] == "FARGATE"
    assert ecs_parameters["NetworkConfiguration"]["awsvpcConfiguration"]["Subnets"]


def test_dockerignore_excludes_local_secret_files():
    dockerignore = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()

    assert ".env" in dockerignore
    assert "dbt/profiles.yml" in dockerignore
    assert "docs/screenshots/" in dockerignore
