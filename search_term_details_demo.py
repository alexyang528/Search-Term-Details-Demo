import streamlit as st
from snowflake import connector
from yext import YextClient

st.set_page_config(page_title="Search Term Details Demo", layout="wide")


"""
# Search Term Details Prototype
This app is an interactive demo of the updated Search Term Details Page.
"""

# Search Term and Inputs
st.sidebar.write("### Experience / Search Term Info")
business_id = st.sidebar.text_input("Business ID", value=3470691)
experience_key = st.sidebar.text_input("Experience Key", value="samsung-support-answers")
api_key = st.sidebar.text_input("API Key", value="4303f1eee26f1c2df76282a4e1981980")
search_term = st.sidebar.text_input("Search Term (Normalized)")
days = st.sidebar.number_input("Last __ Days", min_value=0, value=30, step=1)

st.sidebar.write("### Snowflake Info")
acct = st.sidebar.text_input("Snowflake Account", value="tw61901.us-east-1")
user = st.sidebar.text_input("Snowflake User", value="alyang@yext.com")


def _flatten(values):
    out = []
    for value in values:
        if isinstance(value, list):
            out.extend(_flatten(value))
        else:
            out.append(value)
    return out


def make_clickable(text, link):
    return f'<a target="_blank" href="{link}">{text}</a>'


@st.experimental_singleton
def init_connection(acct, user):
    return connector.connect(
        authenticator="externalbrowser", account=acct, user=user, warehouse="HUMAN_WH"
    )


@st.experimental_singleton
def init_yext_client(api_key):
    return YextClient(api_key, env="PRODUCTION")


conn = init_connection(acct, user)
yext_client = init_yext_client(api_key)


@st.experimental_memo(ttl=600)
def get_data(query):
    with conn.cursor() as curs:
        curs.execute(query)
        return curs.fetch_pandas_all()


if business_id and experience_key and api_key and search_term:
    analytics_query = f"""
    select
        date(searches.timestamp) as date,
        user_data.latitude as "latitude",
        user_data.longitude as "longitude",
        count(distinct user_data.session_id) as sessions,
        count(distinct searches.query_id) as searches,
        count(case when user_event_types.is_click_event then 1 end) as clicks,
        count(distinct case when user_event_types.is_click_event then searches.query_id end) as searches_w_clicks,
        count(distinct case when searches.has_kg_results then searches.query_id end) as searches_w_kg
    from
        searches
        left join user_data on searches.id = user_data.search_id
        left join user_events on searches.id = user_events.search_id
        left join user_event_types on user_events.user_event_type_id = user_event_types.id
    where
        date(searches.timestamp) > dateadd('day', -30, current_date())
        and user_data.traffic_source = 'EXTERNAL'
        and searches.version_label = 'PRODUCTION'
        and searches.business_id = {business_id}
        and searches.experience_key = '{experience_key}'
        and searches.tokenizer_normalized_query = '{search_term}'
    group by 1,2,3
    """
    data = get_data(analytics_query)

    # Analytics
    st.write("## Search Term Analytics")
    st.write("Key metrics regarding search volume and engagement for this search term.")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(label="Searches", value=int(data["SEARCHES"].sum()))
    c2.metric(label="Sessions", value=int(data["SESSIONS"].sum()))
    c3.metric(label="Clicks", value=int(data["CLICKS"].sum()))
    try:
        ctr = int(data["SEARCHES_W_CLICKS"].sum()) / int(data["SEARCHES"].sum())
        ctr = round(ctr, 2)
    except ZeroDivisionError:
        ctr = 0
    c4.metric(label="CTR", value=ctr)

    line_data = data.groupby("DATE").agg({"SEARCHES": sum, "SESSIONS": sum, "CLICKS": sum})
    st.line_chart(line_data)

    # Map
    st.write("## Search Map")
    st.write("Searches for this search term by location.")
    st.map(data)
    with st.expander("Snowflake Query", expanded=False):
        st.code(analytics_query, language="sql")

    # Clustering
    c1, c2, c3 = st.columns(3)

    c1.write("## Related Search Terms")
    c1.write("Related search terms, which are in the same cluster as this search term.")

    cluster_query = f"""
    with cluster as (
        select business_id, experience_key, cluster_id
        from current_cluster_search_terms
        where
            search_term = '{search_term}'
            and business_id = {business_id}
            and experience_key = '{experience_key}'
            and not is_noise
            and is_overlarge is null
    )
    select
        search_term,
        count(distinct searches.query_id) as searches,
        count(distinct user_data.session_id) as sessions,
        count(case when user_event_types.is_click_event then 1 end) as clicks,
        count(distinct case when user_event_types.is_click_event then searches.query_id end) as searches_w_clicks
    from
        cluster
        join current_cluster_search_terms using (business_id, experience_key, cluster_id)
        left join searches on searches.business_id = cluster.business_id and searches.experience_key = cluster.experience_key and searches.tokenizer_normalized_query = current_cluster_search_terms.search_term
        left join user_data on searches.id = user_data.search_id
        left join user_events on searches.id = user_events.search_id
        left join user_event_types on user_events.user_event_type_id = user_event_types.id
    where
        date(searches.timestamp) > dateadd('day', -30, current_date())
        and user_data.traffic_source = 'EXTERNAL'
        and searches.version_label = 'PRODUCTION'
    group by 1
    order by 2 desc
    limit 10
    """
    cluster_data = get_data(cluster_query)
    cluster_data["CTR"] = cluster_data["SEARCHES_W_CLICKS"].divide(cluster_data["SEARCHES"])
    cluster_data = cluster_data[["SEARCH_TERM", "SEARCHES", "SESSIONS", "CLICKS", "CTR"]]

    if len(cluster_data.index) != 0:
        c1.write(cluster_data.to_html(index=False, escape=False), unsafe_allow_html=True)
    else:
        c1.write("_Search term is not part of a cluster._")

    # Most Clicked Results
    c2.write("## Popular Results")
    c2.write("The most clicked results for this search term, sorted by popularity.")

    results_query = f"""
    select
        results.entity_id,
        count(distinct searches.query_id) as searches,
        count(distinct user_data.session_id) as sessions,
        count(case when user_event_types.is_click_event then 1 end) as clicks,
        count(distinct case when user_event_types.is_click_event then searches.query_id end) as searches_w_clicks
    from
        searches
        left join user_data on searches.id = user_data.search_id
        left join vertical_searchers on searches.id = vertical_searchers.search_id
        left join results on vertical_searchers.id = results.vertical_searcher_id
        left join user_events on searches.id = user_events.search_id and vertical_searchers.id = user_events.vertical_searcher_id and results.entity_id = user_events.entity_id
        left join user_event_types on user_events.user_event_type_id = user_event_types.id
    where
        date(searches.timestamp) > dateadd('day', -30, current_date())
        and user_data.traffic_source = 'EXTERNAL'
        and searches.version_label = 'PRODUCTION'
        and searches.business_id = {business_id}
        and searches.experience_key = '{experience_key}'
        and searches.tokenizer_normalized_query = '{search_term}'
    group by 1
    having clicks > 0
    order by clicks desc, searches desc
    limit 10
    """
    results_data = get_data(results_query)

    # Run search term as a query to get entities and names
    results = yext_client.search_answers_universal(query=search_term, experience_key=experience_key)
    response = results.raw_response["response"]
    results = [m["results"] for m in response["modules"]]
    results = _flatten(results)
    entities = [r["data"] for r in results if "data" in r]

    id_name_dict = {}
    for d in entities:
        if "uid" in d and "name" in d:
            id_name_dict[d["uid"]] = d["name"]

    results_data["CTR"] = results_data["SEARCHES_W_CLICKS"].divide(results_data["SEARCHES"])
    results_data["NAME"] = results_data["ENTITY_ID"].apply(lambda x: id_name_dict.get(str(x), None))
    results_data["ENTITY_ID"] = results_data["ENTITY_ID"].apply(
        lambda x: make_clickable(
            x, f"https://www.yext.com/s/{business_id}/entity/edit3?entityIds={x}"
        )
    )

    results_data = results_data[["ENTITY_ID", "NAME", "SEARCHES", "SESSIONS", "CLICKS", "CTR"]]
    c2.write(results_data.to_html(index=False, escape=False), unsafe_allow_html=True)

    # Search Log
    c3.write("## Recent Searches")
    c3.write("A log of the most recent searches for this search term.")

    log_query = f"""
    select
        searches.timestamp,
        searches.query_id,
        concat(user_data.city, ', ', user_data.region) as city,
        user_data.country,
        concat(user_data.latitude, ', ', user_data.longitude) as "LAT, LONG"
    from searches
    left join user_data on searches.id = user_data.search_id
    where
        date(searches.timestamp) > dateadd('day', -30, current_date())
        and user_data.traffic_source = 'EXTERNAL'
        and searches.version_label = 'PRODUCTION'
        and searches.business_id = {business_id}
        and searches.experience_key = '{experience_key}'
        and searches.tokenizer_normalized_query = '{search_term}'
    order by 1 desc
    limit 10
    """
    log_data = get_data(log_query)

    log_data["QUERY_ID"] = log_data["QUERY_ID"].apply(
        lambda x: make_clickable(
            x,
            f"https://www.yext.com/s/{business_id}/answers/experiences/{experience_key}/searchQueryLogDetails/{x}",
        )
    )

    c3.write(log_data.to_html(index=False, escape=False), unsafe_allow_html=True)

    st.write("")

    c1, c2, c3 = st.columns(3)
    with c1.expander("Snowflake Query", expanded=False):
        st.code(cluster_query, language="sql")
    with c2.expander("Snowflake Query", expanded=False):
        st.code(results_query, language="sql")
    with c3.expander("Snowflake Query", expanded=False):
        st.code(log_query, language="sql")
