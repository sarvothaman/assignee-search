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
        host = st.text_input("Host", value="https://patentsview-production.es.us-east-1.aws.found.io")
        api_key = st.text_input("API Key", value="", help="API Key for authentication.")
    
    with st.expander("Configuration", expanded=True):
        timeout = st.number_input("Timeout", value=30, help="Search timeout in seconds.")
        index = st.text_input("Index", value="patents", help="Index to search in.")
        fuzziness = st.number_input("Fuzziness", value=2, help="Fuzziness level for matching.", min_value=0, max_value=2)
        col_select_placeholder = st.empty()

    with st.expander("Search Fields (comma separated):", expanded=False):
        #fields = parse_csv(st.text_input("Search Fields", value="assignees.assignee_organization", help="Fields to search."))
        source = parse_csv(st.text_input("Source", value="", help="Fields to return in the response."))
        agg_fields = parse_csv(st.text_input("Aggregation Fields", value="assignees.assignee_id", help="Fields to aggregate on."))
        agg_source = parse_csv(st.text_input("Aggregation Source", value="assignees", help="Fields to return for each top hit in the aggregations."))

es = ElasticSearch(host, api_key=api_key)
user_query = st.text_input("Search:", value="Lutron Electronics", disabled=api_key=="")
field_select = st.radio("Fields:", ["Organization", "First Name", "Last Name"], horizontal=True, label_visibility="collapsed")
if field_select == "Organization":
    fields = ["assignees.assignee_organization"]
elif field_select == "First Name":
    fields = ["assignees.assignee_individual_name_first"]
elif field_select == "Last Name":
    fields = ["assignees.assignee_individual_name_last"]

with st.spinner('Searching...'):
    try:
        if api_key == "":
            st.write("**Please enter an API Key.**")
            st.stop()
        else:
            results = es.search(user_query, index, fields, agg_fields=agg_fields, source=source, agg_source=agg_source, timeout=timeout, size=0, fuzziness=fuzziness)  # Setting size=0 to only return aggregations
            st.json(results)
    except Exception as e:
        st.error("Could not complete the search!", icon="🚨")
        st.error(e)
        st.exit()

    # Parse results into dataframe
    df = parse_results(results)
    cols = df.columns
    print(cols.values)
    default_cols = [
        'assignee_organization', 
        'assignee_individual_name_last',
        'assignee_individual_name_first',
        'assignee_country',
        'assignee_state',
        'assignee_city',
        'assignee_type', 
        'assignee_id',
        '_score',
    ]
    col_select = col_select_placeholder.multiselect("Columns to display:", options=cols, default=default_cols)

    # Add editable column to indicate selection option
    df.insert(0, "Select", False)
    edited_df = st.data_editor(df[["Select"]+col_select])

    # Search statistics
    entity_count = len(results["aggregations"]["assignees.assignee_id"]["assignees.assignee_id_inner"]["buckets"])
    record_count = results["aggregations"]["assignees.assignee_id"]["doc_count"]
    st.write(f"Found {entity_count} disambiguated assignees with {record_count} associated records.")

    st.write("Selected Assignee IDs:")

    st.write((df[edited_df["Select"] == True][["assignee_id", "assignee_organization"]]))