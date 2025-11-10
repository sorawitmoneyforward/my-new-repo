import os
import sys
import yaml
from jinja2 import Environment, FileSystemLoader
from pydantic import BaseModel


class DatasetConfig(BaseModel):
    catalog: str
    current_date: str
    schema_rules: dict[str, str]

    @classmethod
    def load(cls, env: str, file_path: str) -> "DatasetConfig":
        with open(file_path, "r") as f:
            data = yaml.safe_load(f)
        return cls(**data[env])

    def _schema(self, target_schema: str) -> str:
        return self.schema_rules[target_schema]

    def _table(self, target_schema: str, target_table: str) -> str:
        mapped_schema = self.schema_rules[target_schema]
        if mapped_schema == target_schema:
            return target_table
        return f"{target_schema}__{target_table}"

    def resolve(self, target_schema: str, target_table: str) -> str:
        schema = self._schema(target_schema)
        table = self._table(target_schema, target_table)
        return f"`{self.catalog}`.`{schema}`.`{table}`"


def get_environment() -> str:
    """Determine active environment."""
    env = os.getenv("ENV")
    if env is None:
        raise ValueError(
            "ENV is not set; please set ENV to a valid environment"
        )
    return env


def get_resolved_sql(
    template_name: str,
    environment: str,
    config_path: str,
    template_dir: str,
) -> str:
    """Load and render Jinja2 SQL template."""
    # Load dataset config
    resolver = DatasetConfig.load(env=environment, file_path=config_path)

    # Setup Jinja2 environment
    jinja_env = Environment(loader=FileSystemLoader(template_dir))
    jinja_env.globals["resolve"] = (
        lambda schema, table: resolver.resolve(schema, table)
    )
    jinja_env.globals["current_date"] = resolver.current_date

    # Load and render template (require full name ending with .sql.j2)
    if not template_name.endswith('.sql.j2'):
        raise ValueError(
            "Invalid SQL template name. Expected a filename ending "
            "with '.sql.j2'"
        )
    template = jinja_env.get_template(template_name)
    rendered_sql = template.render()

    return rendered_sql


def get_bucket_name() -> str:
    """Get S3 bucket name from environment variable BUCKET_NAME."""
    bucket_name = os.getenv("BUCKET_NAME")
    if not bucket_name:
        raise ValueError(
            "BUCKET_NAME is not set; "
            "please set BUCKET_NAME to a valid S3 bucket"
        )
    return bucket_name


# TODO: Fix this function in the future
def is_daily_balances_v2() -> bool:
    """Check if daily balances v2 flag is set from sys.argv."""
    # Fallback to sys.argv for backward compatibility
    if len(sys.argv) > 1:
        return sys.argv[1].lower() == "true"
    return False
