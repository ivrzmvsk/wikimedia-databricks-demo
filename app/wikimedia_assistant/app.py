"""Streamlit entry point deployed only after successful corpus sync."""
import json
import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
import streamlit as st

from retrieval import ServiceError, answer, document_path, retrieve


st.set_page_config(page_title="Wikimedia Assistant", page_icon="📚")
st.title("Wikimedia Assistant")
st.caption("Answers from selected Wikipedia summaries and project documentation.")
try:
    config = json.loads(Path("runtime.json").read_text())
    if config["status"] != "READY" or not config.get("generation"):
        raise ValueError("Corpus is not READY")
    index_name, model = os.environ["VECTOR_SEARCH_INDEX"], os.environ["LLM_ENDPOINT"]
    client = WorkspaceClient()
except (OSError, KeyError, ValueError):
    st.error("The collection is not ready or application resources are not configured.")
    st.stop()

st.sidebar.write(f"Collection: {config['corpus_id']}")
st.sidebar.write(f"UTC interval: [{config['start']}, {config['end']})")
project_docs = [d for d in config["documents"] if d.get("document_type") == "project_document"]
st.sidebar.write(f"Wikipedia articles: {len(config['documents']) - len(project_docs)}")
st.sidebar.write(f"Project documents: {len(project_docs)}")
st.sidebar.write(f"Prepared: {config['prepared_at']}")
with st.sidebar.expander("Sources"):
    for document in config["documents"]:
        st.write(f"{document['canonical_title']} — snapshot {document['fetched_at']}, version {document['revision'][:16]}")
st.sidebar.caption("Exact platform rankings and counts are available through Gold SQL.")
if st.sidebar.button("Clear history"):
    st.session_state.messages = []
if "messages" not in st.session_state:
    st.session_state.messages = []


def render(message, message_index):
    st.markdown(message["content"])
    for source_index, source in enumerate(message.get("sources", [])):
        relative = document_path(source["url"])
        if relative:
            path = Path("documents") / relative
            if path.is_file():
                st.download_button(source["title"], data=path.read_text(encoding="utf-8"),
                                   file_name=path.name, mime="text/markdown",
                                   key=f"source-{message_index}-{source_index}")
            else:
                st.caption(f"Source file unavailable: {relative}")
        else:
            st.link_button(source["title"], source["url"])
        st.caption(f"Snapshot: {source['fetched_at']}; version: {source['revision'][:16]}")


for message_index, message in enumerate(st.session_state.messages):
    with st.chat_message(message["role"]):
        render(message, message_index)
question = st.chat_input("Ask about Wikipedia articles or this project", max_chars=2000)
if question:
    st.session_state.messages.append(dict(role="user", content=question))
    with st.chat_message("user"):
        st.write(question)
    with st.chat_message("assistant"):
        try:
            with st.spinner("Searching sources…"):
                rows = retrieve(client, index_name, question, config["corpus_id"], config["generation"])
                result = answer(client, model, question, rows)
            message = dict(role="assistant", content=result["answer"], sources=result["sources"])
            render(message, len(st.session_state.messages))
            st.session_state.messages.append(message)
            st.session_state.messages = st.session_state.messages[-40:]
        except (ServiceError, ValueError) as exc:
            st.error(str(exc))
