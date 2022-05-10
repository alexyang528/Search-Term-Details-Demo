import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from snowflake import connector
from yext import YextClient
from utils import *

st.set_page_config(page_title="Search Term Details Demo", layout="wide")
pd.set_option("display.max_columns", None)

# Get query parameters
QUERY_PARAMS = st.experimental_get_query_params()

# Set initial query param values
def _check_param(k, default):
    if k not in QUERY_PARAMS:
        _update_params(k, default)


# callback to update query param, either to specified value (on init), or from session state key
def _update_params(k, v=None):
    if not v:
        v = st.session_state[k]

    QUERY_PARAMS[k] = [str(v)]
    st.experimental_set_query_params(**QUERY_PARAMS)


# Sidebar Inputs
st.sidebar.write("# Search Experience Info")

_check_param("b", st.secrets["sample"]["business_id"])
_check_param("e", st.secrets["sample"]["exp_key"])
_check_param("a", st.secrets["sample"]["api_key"])
_check_param("mode", "Search Term")

TABS = [
    "Related Search Terms",
    "Most Popular Results",
    "Most Popular Verticals",
    "Integration Source",
    "Search Logs",
]

BUSINESS_ID = st.sidebar.text_input(
    "Business ID",
    key="b",
    value=QUERY_PARAMS["b"][0],
    on_change=_update_params,
    args=("b",),
)
EXPERIENCE_KEY = st.sidebar.text_input(
    "Experience Key",
    key="e",
    value=QUERY_PARAMS["e"][0],
    on_change=_update_params,
    args=("e",),
)
API_KEY = st.sidebar.text_input(
    "API Key", key="a", value=QUERY_PARAMS["a"][0], on_change=_update_params, args=("a",)
)
MODE_OPTIONS = ["Search Term", "Cluster"]
MODE = st.sidebar.selectbox(
    "Mode (ST or Cluster)",
    key="mode",
    options=MODE_OPTIONS,
    index=MODE_OPTIONS.index(QUERY_PARAMS["mode"][0]),
    on_change=_update_params,
    args=("mode",),
)


@st.experimental_singleton
def _init_connection():
    return connector.connect(
        authenticator="https://yext.okta.com",
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        warehouse="HUMAN_WH",
        password=st.secrets["snowflake"]["pass"],
    )


# Connect to Yext Client (for results)
@st.experimental_singleton
def _init_yext_client(API_KEY):
    return YextClient(API_KEY, env="PRODUCTION")


YEXT_CLIENT = _init_yext_client(API_KEY)
CONN = _init_connection()


@st.experimental_memo(ttl=600)
def get_data(query):
    with CONN.cursor() as curs:
        curs.execute(query)
        return curs.fetch_pandas_all()


@st.experimental_memo(ttl=600)
def get_results(search_term):
    # Run search term as a query to get entities and names
    results = YEXT_CLIENT.search_answers_universal(query=search_term, experience_key=EXPERIENCE_KEY)
    response = results.raw_response["response"]
    return response


def get_result_card(result):
    return f"""
        **Entity ID:** {result['data']['id']} \n
        **Name:** {result['data']['name']}
        """


# Check if existing filters selected in query params
_check_param("d", "Last 30 Days")
_check_param("l", "PRODUCTION")
_check_param("t", "External")

# Get Popular Search Terms
filter = {
    "b": QUERY_PARAMS["b"][0],
    "e": QUERY_PARAMS["e"][0],
    "d": MAP[QUERY_PARAMS["d"][0]],
    "l": MAP[QUERY_PARAMS["l"][0]],
    "t": MAP[QUERY_PARAMS["t"][0]],
}
popular = get_data(PARAMS["popular_query"][MODE].format(**filter))[
    PARAMS["popular_query_col"][MODE]
].tolist()

# Display Search Terms Select
_check_param("s", popular[0])
if QUERY_PARAMS["s"][0] not in popular:
    _update_params("s", popular[0])

term = st.sidebar.selectbox(
    "Popular Search Term",
    options=popular,
    key="s",
    index=popular.index(QUERY_PARAMS["s"][0]),
    on_change=_update_params,
    args=("s",),
)

# Display Headers
st.markdown(
    "#####"
    f" [{PARAMS['head'][MODE]}]({PARAMS['url'][MODE].format(st.session_state.b, st.session_state.e)})"
    f" / {term}"
)
st.markdown(f"# **{term}**")

# Display Filter Bar
c1, c2, c3, _, __ = st.columns([2, 2, 2, 0.3, 3])

dates = c1.selectbox(
    "Search Date",
    key="d",
    options=DATE_OPTIONS,
    index=DATE_OPTIONS.index(QUERY_PARAMS["d"][0]),
    on_change=_update_params,
    args=("d",),
)
label = c2.selectbox(
    "Configuration Label",
    key="l",
    options=LABEL_OPTIONS,
    index=LABEL_OPTIONS.index(QUERY_PARAMS["l"][0]),
    on_change=_update_params,
    args=("l",),
)
traffic = c3.selectbox(
    "Traffic Type",
    key="t",
    options=TRAFFIC_OPTIONS,
    index=TRAFFIC_OPTIONS.index(QUERY_PARAMS["t"][0]),
    on_change=_update_params,
    args=("t",),
)

# Page-level Columns
st.markdown("""---""")
analytics, _, test_search = st.columns([2, 0.1, 1])

# Analytics Overview
filter = {
    "b": st.session_state.b,
    "e": st.session_state.e,
    "s": st.session_state.s,
    "d": MAP[st.session_state.d],
    "l": MAP[st.session_state.l],
    "t": MAP[st.session_state.t],
}
data = get_data(PARAMS["analytics_query"][MODE].format(**filter))

heros = go.Figure()
heros.add_trace(
    go.Indicator(
        value=int(data["SEARCHES"].sum()), title="Searches", domain={"row": 0, "column": 0}
    )
)
heros.add_trace(
    go.Indicator(
        value=int(data["SESSIONS"].sum()), title="Sessions", domain={"row": 0, "column": 1}
    )
)
heros.add_trace(
    go.Indicator(value=int(data["CLICKS"].sum()), title="Clicks", domain={"row": 0, "column": 2})
)
try:
    avg_ctr = round(data["CTR"].dot(data["SEARCHES"]) / data["SEARCHES"].sum(), 2)
except:
    avg_ctr = 0
heros.add_trace(
    go.Indicator(
        value=avg_ctr,
        title="CTR",
        domain={"row": 0, "column": 3},
    )
)
try:
    avg_kg_result = round(data["KG_RESULT_RATE"].dot(data["SEARCHES"]) / data["SEARCHES"].sum(), 2)
except:
    avg_ctr = 0
heros.add_trace(
    go.Indicator(
        value=avg_kg_result,
        title="KG Result Rate",
        domain={"row": 0, "column": 4},
    )
)

heros.update_layout(grid={"rows": 1, "columns": 5}, margin=dict(t=0, b=0, pad=0), height=200)
analytics.plotly_chart(heros, use_container_width=True)

line_data = data.groupby("DATE").agg({"SEARCHES": sum, "SESSIONS": sum, "CLICKS": sum})
line_graph = go.Figure()
line_graph.add_trace(
    go.Scatter(
        x=line_data.index,
        y=line_data["SEARCHES"],
        name="Searches",
        hoverinfo="name+y",
        line_shape="spline",
    )
)
line_graph.add_trace(
    go.Scatter(
        x=line_data.index,
        y=line_data["SESSIONS"],
        name="Sessions",
        hoverinfo="name+y",
        line_shape="spline",
    )
)
line_graph.add_trace(
    go.Scatter(
        x=line_data.index,
        y=line_data["CLICKS"],
        name="Clicks",
        hoverinfo="name+y",
        line_shape="spline",
    )
)
line_graph.update_layout(margin=dict(t=0, b=0, pad=0))
analytics.plotly_chart(line_graph, use_container_width=True)

analytics.markdown("""---""")

active_tab = analytics.radio("", TABS, index=0, key="tabs")
child = TABS.index(active_tab) + 1
analytics.markdown(
    """
        <style type="text/css">
        div[role=radiogroup] > label > div:first-of-type, .stRadio > label {
            display: none;
        }
        div[role=radiogroup] {
            flex-direction: unset
        }
        div[role=radiogroup] label {
            border-bottom: 1px solid #999;
            background: #FFF !important;
            padding: 4px 12px;
            border-radius: 4px 4px 0 0;
            position: relative;
            top: 8px;
            font-size: 18px;
            font-weight: bold !important;
            }
        div[role=radiogroup] label:nth-child("""
    + str(child)
    + """) {
            background: #FFF !important;
            border-bottom: 2px solid #1564F9;
            font-weight: 900;
        }
        </style>
    """,
    unsafe_allow_html=True,
)

# Related Search Terms Module
if active_tab == "Related Search Terms":
    if MODE == "Search Term":
        analytics.write("Other search terms in the same cluster as this search term.")
    else:
        analytics.write("Search terms in this cluster.")

    cluster_data = get_data(PARAMS["cluster_query"][MODE].format(**filter))
    cluster_data = cluster_data[["SEARCH_TERM", "SEARCHES", "SESSIONS", "CLICKS", "CTR"]]

    if len(cluster_data.index) != 0:
        analytics.write(
            cluster_data.to_html(index=False, escape=False, justify="left"),
            unsafe_allow_html=True,
        )
    else:
        if MODE == "Search Term":
            analytics.write("_Search term is not part of a cluster._")
        else:
            analytics.write("No search terms in this cluster for the selected filters.")

    analytics.markdown("""---""")
    with analytics.expander("Snowflake Queries", expanded=False):
        st.write("Analytics Overview:")
        st.code(PARAMS["analytics_query"][MODE].format(**filter), language="sql")
        st.write("Details Query:")
        st.code(PARAMS["cluster_query"][MODE].format(**filter), language="sql")

# Most Popular Results Module
elif active_tab == "Most Popular Results":
    analytics.write(f"The most clicked results for this {MODE.lower()}, sorted by popularity.")

    results_data = get_data(PARAMS["results_query"][MODE].format(**filter))

    response = get_results(term)
    km_modules = [m for m in response["modules"] if m["source"] == "KNOWLEDGE_MANAGER"]
    results = [m["results"] for m in km_modules]
    all_results = flatten(results)
    all_entities = [r["data"] for r in all_results if "data" in r]

    id_name_dict = {}
    for d in all_entities:
        if "uid" in d and "name" in d:
            id_name_dict[d["uid"]] = d["name"]

    results_data["NAME"] = results_data["ENTITY_ID"].apply(lambda x: id_name_dict.get(str(x), ""))
    results_data["ENTITY_ID"] = results_data["ENTITY_ID"].apply(
        lambda x: make_clickable(
            x, f"https://www.yext.com/s/{BUSINESS_ID}/entity/edit3?entityIds={x}"
        )
    )
    results_data = results_data[["ENTITY_ID", "NAME", "SEARCHES", "SESSIONS", "CLICKS", "CTR"]]

    if len(results_data.index) != 0:
        analytics.write(
            results_data.to_html(index=False, escape=False, justify="left"),
            unsafe_allow_html=True,
        )
    else:
        analytics.write(
            f"_No entities have been clicked for this {MODE.lower()} with the selected filters._"
        )

    analytics.markdown("""---""")
    with analytics.expander("Snowflake Queries", expanded=False):
        st.write("Analytics Overview:")
        st.code(PARAMS["analytics_query"][MODE].format(**filter), language="sql")
        st.write("Details Query:")
        st.code(PARAMS["results_query"][MODE].format(**filter), language="sql")

# Most Popular Vertical Module
elif active_tab == "Most Popular Verticals":
    analytics.write(f"The most clicked verticals for this {MODE.lower()}, sorted by popularity.")

    vertical_data = get_data(PARAMS["vertical_query"][MODE].format(**filter))

    if len(vertical_data.index) != 0:
        analytics.write(
            vertical_data.to_html(index=False, escape=False, justify="left"),
            unsafe_allow_html=True,
        )
    else:
        analytics.write(
            f"_No verticals clicked for this {MODE.lower()} with the selected filters._"
        )

    analytics.markdown("""---""")
    with analytics.expander("Snowflake Queries", expanded=False):
        st.write("Analytics Overview:")
        st.code(PARAMS["analytics_query"][MODE].format(**filter), language="sql")
        st.write("Details Query:")
        st.code(PARAMS["vertical_query"][MODE].format(**filter), language="sql")

# Integration Source Module
elif active_tab == "Integration Source":
    analytics.write("Search volume and engagement by custom integration source.")

    source_data = get_data(PARAMS["source_query"][MODE].format(**filter))

    if len(source_data.index) != 0:
        analytics.write(
            source_data.to_html(index=False, escape=False, justify="left"),
            unsafe_allow_html=True,
        )
    else:
        analytics.write(
            f"_No searches on any integration sources for this {MODE.lower()} with the selected"
            " filters._"
        )

    analytics.markdown("""---""")
    with analytics.expander("Snowflake Queries", expanded=False):
        st.write("Analytics Overview:")
        st.code(PARAMS["analytics_query"][MODE].format(**filter), language="sql")
        st.write("Details Query:")
        st.code(PARAMS["source_query"][MODE].format(**filter), language="sql")

# Search Log Module
elif active_tab == "Search Logs":
    analytics.write(f"A log of the most recent searches for this {MODE.lower()}.")

    log_data = get_data(PARAMS["logs_query"][MODE].format(**filter))

    log_data["QUERY_ID"] = log_data["QUERY_ID"].apply(
        lambda x: make_clickable(
            x,
            f"https://www.yext.com/s/{BUSINESS_ID}/answers/experiences/{EXPERIENCE_KEY}/searchQueryLogDetails/{x}",
        )
    )

    if len(log_data.index) != 0:
        analytics.write(
            log_data.to_html(index=False, escape=False, justify="left"), unsafe_allow_html=True
        )
    else:
        analytics.write(f"_No recent searches this {MODE.lower()} with the selected filters._")

    analytics.markdown("""---""")
    with analytics.expander("Snowflake Queries", expanded=False):
        st.write("Analytics Overview:")
        st.code(PARAMS["analytics_query"][MODE].format(**filter), language="sql")
        st.write("Details Query:")
        st.code(PARAMS["logs_query"][MODE].format(**filter), language="sql")
else:
    st.error("Something has gone terribly wrong.")

test_search.write("#### **Test Search**")
test_search_term = test_search.text_input("", f"{term}")
test_search.markdown("""---""")

# Get Results for the Selected Search Term
if test_search_term:
    response = get_results(test_search_term)
    km_modules = [m for m in response["modules"] if m["source"] == "KNOWLEDGE_MANAGER"]
    verticals = [m["verticalConfigId"] for m in km_modules]
    results = [m["results"] for m in km_modules]
    results_dict = dict(zip(verticals, results))
    all_results = flatten(results)
    all_entities = [r["data"] for r in all_results if "data" in r]

    for vertical in results_dict:
        test_search.warning(f"**{vertical}**")
        count = 0
        for result in results_dict[vertical]:
            if count > 4:
                break
            test_search.info(get_result_card(result))
            count += 1
