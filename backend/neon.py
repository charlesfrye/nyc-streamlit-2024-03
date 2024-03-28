from pathlib import Path

import modal

neon_image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install(
        "psycopg2-binary~=2.9.9",
        "pandas",
        "sqlalchemy",
        "requests~=2.31.0",
    )
    .apt_install("wget", "lsb-release")
    .run_commands(
        [
            "sh -c 'echo \"deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main\" > /etc/apt/sources.list.d/postgres.list'",
            "wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -",
        ]
    )
    .apt_install("postgresql-client-14")
    .run_commands(
        [
            "wget --quiet -O - https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash",
        ]
    )
    .env({"NVM_DIR": "/root/.nvm"})
    .run_commands(
        [
            ". $NVM_DIR/nvm.sh && nvm install 20",
            ". $NVM_DIR/nvm.sh && nvm use 20 && node -v && npm -v",
            ". $NVM_DIR/nvm.sh && npm i -g neonctl && neonctl completion >> /root/.bashrc",
        ]
    )
)


stub = modal.Stub(
    "neon-client",
    secrets=[modal.Secret.from_name("my-neon-secret")],
    image=neon_image,
)

with neon_image.imports():
    import os

    import requests
    import sqlalchemy


@stub.cls()
class ApiClient:
    @modal.enter()
    def setup(self):
        self.project_id = os.environ["NEON_PROJECT_ID"]
        self.api_key = os.environ["NEON_API_KEY"]

    @property
    def base_url(self):
        return f"https://console.neon.tech/api/v2/projects/{self.project_id}/branches"

    @property
    def headers(self):
        return {"accept": "application/json", "authorization": f"Bearer {self.api_key}"}

    @modal.method()
    def get_branches(self):
        response = requests.get(self.base_url, headers=self.headers)
        branches = [
            (branch["id"], branch["name"]) for branch in response.json()["branches"]
        ]
        return branches

    @modal.method()
    def get_host(self, branch_id):
        """Find the host name associated with a given branch."""
        url = f"{self.base_url}/{branch_id}/endpoints"
        response = requests.get(url, headers=self.headers)
        host = response.json()["endpoints"][0]["host"]
        return host


@stub.cls()
class DbClient:
    def __init__(self, host):
        self.host = host

    @modal.enter()
    def setup(self):
        self.project_id = os.environ["NEON_PROJECT_ID"]
        self.api_key = os.environ["NEON_API_KEY"]
        self.db_user = os.environ["PGUSER"]
        self.db_password = os.environ["PGPASSWORD"]

    @modal.enter()
    def connect(self):
        """Connect to the Neon pgsql database.

        The `modal.enter` decorator ensures we only do this once per instance."""

        engine = sqlalchemy.create_engine(self.connection_string)
        self.engine = engine

    @modal.method()
    def test_connection(self):
        result = self.execute.local(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' LIMIT 10"
        )
        return result

    @modal.method()
    def execute(self, query, params=None):
        import pandas as pd

        try:
            df = pd.read_sql(query, self.engine, params=params if params else {})
            return df
        except Exception as e:
            print(e)
            return str(e)

    @modal.method()
    def to_sql(self, df, table_name, if_exists, index):
        df.to_sql(
            table_name,
            con=self.engine,
            if_exists=if_exists,
            index=index,
        )

    @property
    def connection_string(self):
        return f"postgresql://{self.db_user}:{self.db_password}@{self.host}/neondb?sslmode=require"

    @modal.method()
    def get_connection_string(self):
        return self.connection_string

    @modal.exit()
    def close(self):
        self.engine.dispose()


@stub.local_entrypoint()
def main(query_file: str = None):
    print("connecting to Neon db")
    api_client = ApiClient()
    branches = api_client.get_branches.remote()
    assert branches, "failed to fetch branches from the api"
    branch_id, branch_name = branches[0]
    print(f"connecting to Neon db for branch {branch_name}")
    host = api_client.get_host.remote(branch_id)
    print(f"found host: {host}")
    client = DbClient(host)
    print(f"connected. listing all tables in branch {branch_name}:")
    print("", *[row[0] for row in client.test_connection.remote()], sep="\t")
    if query_file is not None:
        print(f"applying query from file {query_file}")
        client.from_seed.remote(host, Path(query_file).read_text())
