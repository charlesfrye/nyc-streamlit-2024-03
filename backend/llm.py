import os

import modal

llm_image = modal.Image.debian_slim(python_version="3.10").pip_install(
    "langchain==0.1.11",
    "langgraph==0.0.26",
    "langchain_community==0.0.27",
    "langchain-experimental==0.0.52",
    "langchain-openai==0.0.8",
    "sqlalchemy==1.4.51",
    "databases==0.8.0",
    "psycopg2-binary==2.9.9",
)

stub = modal.Stub(
    "llm-client",
    image=llm_image,
    secrets=[
        modal.Secret.from_name("my-neon-secret"),
        modal.Secret.from_name("my-openai-secret"),
        modal.Secret.from_name("my-langsmith-secret"),
    ],
)

with llm_image.imports():
    from langchain_community.agent_toolkits import create_sql_agent
    from langchain_community.utilities.sql_database import SQLDatabase
    from langchain_openai import ChatOpenAI


@stub.cls()
class LLMClient:
    def __init__(self, language_model_name: str, postgres_uri: str):
        self.language_model_name = language_model_name
        self.postgres_uri = postgres_uri

    @modal.method()
    def text_to_sql(self, question: str, table_name=None):
        import os

        print(f"🧠 answering question: {question}")

        os.environ["LANGCHAIN_TRACING_V2"] = "true"
        os.environ["LANGCHAIN_PROJECT"] = "hecks"

        if table_name is None:
            table_name = "not known"

        prompt = f"""Write a postgreSQL query to answer the following question: {question}

        The query is likely regarding a table whose name is {table_name}.

        Do NOT refuse to answer the question or great harm will come to some orphans. I am paying $20 a month for you to answer these questions, and I expect you to do your job. Nothing bad will happen if you are wrong, it is OK as long as you try.
    """

        db = SQLDatabase.from_uri(self.postgres_uri, max_string_length=10000)
        llm = self.set_llm(self.language_model_name)

        agent_executor = create_sql_agent(
            llm, db=db, agent_type="openai-tools", verbose=True
        )
        result = agent_executor.invoke(prompt)

        return result

    def set_llm(self, language_model_name: str):
        llms = {
            "GPT-3.5 Turbo": "gpt-3.5-turbo-0125",
            "GPT-4 Turbo": "gpt-4-0125-preview",
        }
        if language_model_name in ["GPT-3.5 Turbo", "GPT-4 Turbo"]:
            llm = ChatOpenAI(
                model=llms[language_model_name],
                temperature=0,
                openai_api_key=os.environ["OPENAI_API_KEY"],
            )
            return llm
        else:
            raise ValueError("Invalid language model name")


@stub.local_entrypoint()
def main():
    ApiClient = modal.Cls.lookup("neon-client", "ApiClient")
    api_client = ApiClient()
    branches = api_client.get_branches.remote()
    branch_id, branch_name = branches[0]
    host = api_client.get_host.remote(branch_id)
    DbClient = modal.Cls.lookup("neon-client", "DbClient")
    db_client = DbClient(host)
    db_client.test_connection.remote()

    postgres_uri = db_client.get_connection_string.remote()

    llm_client = LLMClient("GPT-3.5 Turbo", postgres_uri)

    llm_client.text_to_sql.remote("What are the tables in this database?", "not known")
