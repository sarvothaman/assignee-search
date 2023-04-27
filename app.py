import streamlit as st
from er_evaluation.search import ElasticSearch
import pandas as pd

"""
## Disambiguated Assignee Search
"""

with st.expander("Information"):
    st.info("This is a demo search tool for disambiguated assignees. By default, the search is performed on the `assignees.assignee_organization` field, aggregates by disambiguated assignee ID, and returns assignee information for the top hit within each aggregation bucket.")

    st.info("Aggregation searches can be time-consuming. Avoid including short keywords that may match a large number of companies (e.g., 'LLC' or 'Corp'). If needed, increase the search timeout to up to a few minutes.")

def parse_csv(csv):
    return [x.strip() for x in csv.split(",")]

def parse_results(results):
    agg_buckets = results["aggregations"]["assignees.assignee_id"]["assignees.assignee_id_inner"]["buckets"]
    df = pd.DataFrame.from_records(x["top_hits"]["hits"]["hits"][0]["_source"] for x in agg_buckets)
    df["_score"] = [x["top_hits"]["hits"]["hits"][0]["_score"] for x in agg_buckets]
    df.sort_values("_score", ascending=False, inplace=True)
    return df

with st.sidebar:

    with st.expander("ElasticSearch Connection", expanded=True):
        host = st.text_input("Host", value="https://patentsview-production.es.us-east-1.aws.found.io", label_visibility="collapsed")
        api_key = st.text_input("API Key", value="", help="API Key for authentication.")
    
    with st.expander("Configuration", expanded=True):
        timeout = st.number_input("Timeout", value=30, help="Search timeout in seconds.")
        index = st.text_input("Index", value="patents", help="Index to search in.")

    with st.expander("Search Fields (comma separated):", expanded=True):
        fields = parse_csv(st.text_input("Search Fields", value="assignees.assignee_organization", help="Fields to search."))
        source = parse_csv(st.text_input("Source", value="", help="Fields to return in the response."))
        agg_fields = parse_csv(st.text_input("Aggregation Fields", value="assignees.assignee_id", help="Fields to aggregate on."))
        agg_source = parse_csv(st.text_input("Aggregation Source", value="assignees", help="Fields to return for each top hit in the aggregations."))


es = ElasticSearch(host, api_key=api_key)
user_query = st.text_input("Search:", value="Lutron Electronics", disabled=api_key=="")

with st.spinner('Searching...'):
    try:
        if api_key == "":
            st.write("**Please enter an API Key.**")
            st.stop()
        else:
            results = es.search(user_query, index, fields, agg_fields=agg_fields, source=source, agg_source=agg_source, timeout=timeout, size=0)  # Setting size=0 to only return aggregations
    except Exception as e:
        st.error("Could not complete the search!", icon="🚨")
        st.error(e)
        st.exit()

    # Parse results into dataframe
    df = parse_results(results)

    # Add editable column to indicate selection option
    df.insert(0, "Select", False)
    edited_df = st.experimental_data_editor(df)

    # Search statistics
    entity_count = len(results["aggregations"]["assignees.assignee_id"]["assignees.assignee_id_inner"]["buckets"])
    record_count = results["aggregations"]["assignees.assignee_id"]["doc_count"]
    st.write(f"Found {entity_count} disambiguated assignees with {record_count} associated records.")


    copy = st.button("Copy Selected Assignee IDs to Clipboard", type="primary")
    if copy:
        edited_df[edited_df["Select"] == True]["assignee_id"].to_clipboard(index=False, header=False, sep=",")