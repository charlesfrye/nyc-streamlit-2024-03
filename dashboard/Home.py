from pathlib import Path

import modal
import streamlit as st

st.set_page_config(page_icon="🤬", page_title="Hecks")
st.title("🤬 Hecks")

st.divider()

st.markdown(
    """
### Because writing SQL is heckin' hard!

Hecks lets you ask questions about your data in plain English.
    """
)

language_model_name = st.selectbox(
    "🤖 Choose your fighter!",
    ["GPT-3.5 Turbo", "GPT-4 Turbo"],
    key="language_model_name",
)

file = st.file_uploader(
    "Upload a `.csv` file", type=["csv"], accept_multiple_files=False, key="csv"
)


def to_pandas(file):
    import pandas as pd

    return pd.read_csv(file)


def sanitize(name):
    import re

    name = Path(name).stem  # drop extension
    name = re.sub(r"\W+", "_", name)  # remove special chars
    if not name[0].isalpha():  # start with a letter
        name = "t_" + name
    name = name.lower()  # avoid case sensitivity
    name = name[:63]  # truncate to max length
    return name


@st.cache_data
def to_df(file):
    df = to_pandas(file)
    st.session_state["df"] = df
    st.session_state["table_name"] = sanitize(file.name)
    return df


if st.session_state["csv"] is not None:
    df = to_df(st.session_state["csv"])
    df.iloc[:50]


@st.cache_resource
def connect_db():
    ApiClient = modal.Cls.lookup("neon-client", "ApiClient")
    api_client = ApiClient()
    branches = api_client.get_branches.remote()
    branch_id, branch_name = branches[0]
    host = api_client.get_host.remote(branch_id)
    DbClient = modal.Cls.lookup("neon-client", "DbClient")
    client = DbClient(host)
    client.test_connection.remote()

    return client


@st.cache_resource
def connect_llm(language_model_name, neon_uri):
    LLMClient = modal.Cls.lookup("llm-client", "LLMClient")
    client = LLMClient(language_model_name, neon_uri)

    return client


db_client = connect_db()
llm_client = connect_llm(language_model_name, db_client.get_connection_string.remote())


@st.cache_data
def to_sql(df):
    db_client.to_sql.remote(
        df, table_name=sanitize(file.name), if_exists="replace", index=True
    )


def run_query(query):
    language_model_name = st.session_state.get("language_model_name") or "GPT-3.5 Turbo"
    table_name = st.session_state.get("table_name") or "not known"
    if query == "test":
        sql_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' LIMIT 10"
        df = db_client.execute.remote(sql_query)
        return df
    else:
        st.toast(
            f"Converting query: '{query}' to SQL using {language_model_name}", icon="🤖"
        )
        response = llm_client.text_to_sql.remote(query, table_name)["output"]
    return response


if st.session_state.get("df") is not None:
    query = st.text_area(
        "What do you want to know about your data?",
        placeholder="What are the columns in this table?",
        key="query",
    )
    to_sql(st.session_state.get("df"))

if st.session_state.get("query"):
    result = run_query(query)
    if isinstance(result, str):
        st.markdown(result)
    else:
        result
