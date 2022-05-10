from regex import P


def flatten(values):
    out = []
    for value in values:
        if isinstance(value, list):
            out.extend(flatten(value))
        else:
            out.append(value)
    return out


def make_clickable(text, link):
    return f'<a target="_blank" href="{link}">{text}</a>'


DATE_OPTIONS = ["Last 7 Days", "Last 30 Days", "Last 90 Days", "Last 12 Weeks"]
TRAFFIC_OPTIONS = ["All Traffic", "External", "Internal"]
LABEL_OPTIONS = ["All Configuration Labels", "STAGING", "PRODUCTION"]
MAP = {
    "Last 7 Days": -7,
    "Last 30 Days": -30,
    "Last 90 Days": -90,
    "Last 12 Weeks": -84,
    "All Traffic": "in ('EXTERNAL', 'INTERNAL')",
    "External": "= 'EXTERNAL'",
    "Internal": "= 'INTERNAL'",
    "All Configuration Labels": "in ('STAGING','PRODUCTION')",
    "STAGING": "= 'STAGING'",
    "PRODUCTION": "= 'PRODUCTION'",
}


POPULAR_SEARCH_TERMS = """
select tokenizer_normalized_query
from searches
where
    date(searches.timestamp) > dateadd('day', -30, current_date())
    and searches.business_id = {b}
    and searches.experience_key = '{e}'
group by 1
order by count(distinct query_id) desc
limit 50
"""

POPULAR_CLUSTERS = """
select cluster_name
from
    searches
    join current_cluster_search_terms on searches.business_id = current_cluster_search_terms.business_id and searches.experience_key = current_cluster_search_terms.experience_key and searches.tokenizer_normalized_query = current_cluster_search_terms.search_term
where
    date(searches.timestamp) > dateadd('day', -30, current_date())
    and searches.business_id = {b}
    and searches.experience_key = '{e}'
group by 1
order by count(distinct searches.query_id) desc
limit 50
"""


ANALYTICS_QUERY = """
select
    date(searches.timestamp) as date,
    count(distinct user_data.session_id) as sessions,
    count(distinct searches.query_id) as searches,
    count(case when user_event_types.is_click_event then 1 end) as clicks,
    round(div0(count(distinct case when user_event_types.is_click_event then searches.query_id end), count(distinct searches.query_id)), 2) as ctr,
    round(div0(count(distinct case when searches.has_kg_results then searches.query_id end), count(distinct searches.query_id)), 2) as kg_result_rate
from
    searches
    join user_data on searches.id = user_data.search_id
    left join user_events on searches.id = user_events.search_id
    left join user_event_types on user_events.user_event_type_id = user_event_types.id
where
    date(searches.timestamp) > dateadd('day', {d}, current_date())
    and user_data.traffic_source {t}
    and searches.version_label {l}
    and searches.business_id = {b}
    and searches.experience_key = '{e}'
    and searches.tokenizer_normalized_query = '{s}'
group by 1
"""

ANALYTICS_QUERY_C = """
select
    date(searches.timestamp) as date,
    count(distinct user_data.session_id) as sessions,
    count(distinct searches.query_id) as searches,
    count(case when user_event_types.is_click_event then 1 end) as clicks,
    round(div0(count(distinct case when user_event_types.is_click_event then searches.query_id end), count(distinct searches.query_id)), 2) as ctr,
    round(div0(count(distinct case when searches.has_kg_results then searches.query_id end), count(distinct searches.query_id)), 2) as kg_result_rate
from
    searches
    join user_data on searches.id = user_data.search_id
    join current_cluster_search_terms on searches.business_id = current_cluster_search_terms.business_id and searches.experience_key = current_cluster_search_terms.experience_key and searches.tokenizer_normalized_query = current_cluster_search_terms.search_term
    left join user_events on searches.id = user_events.search_id
    left join user_event_types on user_events.user_event_type_id = user_event_types.id
where
    date(searches.timestamp) > dateadd('day', {d}, current_date())
    and user_data.traffic_source {t}
    and searches.version_label {l}
    and searches.business_id = {b}
    and searches.experience_key = '{e}'
    and current_cluster_search_terms.cluster_name = '{s}'
group by 1
"""

CLUSTER_QUERY = """
with cluster as (
    select business_id, experience_key, cluster_id
    from current_cluster_search_terms
    where
        search_term = '{s}'
        and business_id = {b}
        and experience_key = '{e}'
        and not is_noise
        and not is_overlarge
)
select
    search_term,
    count(distinct searches.query_id) as searches,
    count(distinct user_data.session_id) as sessions,
    count(case when user_event_types.is_click_event then 1 end) as clicks,
    round(div0(count(distinct case when user_event_types.is_click_event then searches.query_id end), count(distinct searches.query_id)), 2) as ctr
from
    cluster
    join current_cluster_search_terms using (business_id, experience_key, cluster_id)
    left join searches on searches.business_id = cluster.business_id and searches.experience_key = cluster.experience_key and searches.tokenizer_normalized_query = current_cluster_search_terms.search_term
    left join user_data on searches.id = user_data.search_id
    left join user_events on searches.id = user_events.search_id
    left join user_event_types on user_events.user_event_type_id = user_event_types.id
where
    date(searches.timestamp) > dateadd('day', {d}, current_date())
    and user_data.traffic_source {t}
    and searches.version_label {l}
    and search_term != '{s}'
group by 1
order by 2 desc
limit 10
"""

CLUSTER_QUERY_C = """
select
    current_cluster_search_terms.search_term,
    count(distinct searches.query_id) as searches,
    count(distinct user_data.session_id) as sessions,
    count(case when user_event_types.is_click_event then 1 end) as clicks,
    round(div0(count(distinct case when user_event_types.is_click_event then searches.query_id end), count(distinct searches.query_id)), 2) as ctr
from
    searches
    join user_data on searches.id = user_data.search_id
    join current_cluster_search_terms on searches.business_id = current_cluster_search_terms.business_id and searches.experience_key = current_cluster_search_terms.experience_key and searches.tokenizer_normalized_query = current_cluster_search_terms.search_term
    left join user_events on searches.id = user_events.search_id
    left join user_event_types on user_events.user_event_type_id = user_event_types.id
where
    date(searches.timestamp) > dateadd('day', {d}, current_date())
    and user_data.traffic_source {t}
    and searches.version_label {l}
    and searches.business_id = {b}
    and searches.experience_key = '{e}'
    and current_cluster_search_terms.cluster_name = '{s}'
group by 1
order by 2 desc
limit 10
"""

RESULTS_QUERY = """
select
    results.entity_id,
    count(distinct searches.query_id) as searches,
    count(distinct user_data.session_id) as sessions,
    count(case when user_event_types.is_click_event then 1 end) as clicks,
    round(div0(count(distinct case when user_event_types.is_click_event then searches.query_id end), count(distinct searches.query_id)), 2) as ctr
from
    searches
    left join user_data on searches.id = user_data.search_id
    left join vertical_searchers on searches.id = vertical_searchers.search_id
    left join results on vertical_searchers.id = results.vertical_searcher_id
    left join user_events on searches.id = user_events.search_id and vertical_searchers.id = user_events.vertical_searcher_id and results.entity_id = user_events.entity_id
    left join user_event_types on user_events.user_event_type_id = user_event_types.id
where
    date(searches.timestamp) > dateadd('day', {d}, current_date())
    and user_data.traffic_source {t}
    and searches.version_label {l}
    and searches.business_id = {b}
    and searches.experience_key = '{e}'
    and searches.tokenizer_normalized_query = '{s}'
group by 1
having clicks > 0
order by clicks desc, searches desc
limit 10
"""

RESULTS_QUERY_C = """
select
    results.entity_id,
    count(distinct searches.query_id) as searches,
    count(distinct user_data.session_id) as sessions,
    count(case when user_event_types.is_click_event then 1 end) as clicks,
    round(div0(count(distinct case when user_event_types.is_click_event then searches.query_id end), count(distinct searches.query_id)), 2) as ctr
from
    searches
    join user_data on searches.id = user_data.search_id
    join current_cluster_search_terms on searches.business_id = current_cluster_search_terms.business_id and searches.experience_key = current_cluster_search_terms.experience_key and searches.tokenizer_normalized_query = current_cluster_search_terms.search_term
    left join vertical_searchers on searches.id = vertical_searchers.search_id
    left join results on vertical_searchers.id = results.vertical_searcher_id
    left join user_events on searches.id = user_events.search_id and vertical_searchers.id = user_events.vertical_searcher_id and results.entity_id = user_events.entity_id
    left join user_event_types on user_events.user_event_type_id = user_event_types.id
where
    date(searches.timestamp) > dateadd('day', {d}, current_date())
    and user_data.traffic_source {t}
    and searches.version_label {l}
    and searches.business_id = {b}
    and searches.experience_key = '{e}'
    and current_cluster_search_terms.cluster_name = '{s}'
group by 1
having clicks > 0
order by clicks desc, searches desc
limit 10
"""

VERTICALS_QUERY = """
select
    vertical_searchers.vertical_id,
    count(distinct searches.query_id) as searches,
    count(distinct user_data.session_id) as sessions,
    count(case when user_event_types.is_click_event then 1 end) as clicks,
    div0(count(distinct case when user_event_types.is_click_event then searches.query_id end), count(distinct searches.query_id)) as ctr
from
    searches
    left join user_data on searches.id = user_data.search_id
    left join vertical_searchers on searches.id = vertical_searchers.search_id
    left join user_events on searches.id = user_events.search_id and vertical_searchers.id = user_events.vertical_searcher_id
    left join user_event_types on user_events.user_event_type_id = user_event_types.id
where
    date(searches.timestamp) > dateadd('day', {d}, current_date())
    and user_data.traffic_source {t}
    and searches.version_label {l}
    and searches.business_id = {b}
    and searches.experience_key = '{e}'
    and searches.tokenizer_normalized_query = '{s}'
group by 1
having clicks > 0
order by clicks desc, searches desc
limit 10
"""

VERTICALS_QUERY_C = """
select
    vertical_searchers.vertical_id,
    count(distinct searches.query_id) as searches,
    count(distinct user_data.session_id) as sessions,
    count(case when user_event_types.is_click_event then 1 end) as clicks,
    div0(count(distinct case when user_event_types.is_click_event then searches.query_id end), count(distinct searches.query_id)) as ctr
from
    searches
    join user_data on searches.id = user_data.search_id
    join current_cluster_search_terms on searches.business_id = current_cluster_search_terms.business_id and searches.experience_key = current_cluster_search_terms.experience_key and searches.tokenizer_normalized_query = current_cluster_search_terms.search_term
    left join vertical_searchers on searches.id = vertical_searchers.search_id
    left join user_events on searches.id = user_events.search_id and vertical_searchers.id = user_events.vertical_searcher_id
    left join user_event_types on user_events.user_event_type_id = user_event_types.id
where
    date(searches.timestamp) > dateadd('day', {d}, current_date())
    and user_data.traffic_source {t}
    and searches.version_label {l}
    and searches.business_id = {b}
    and searches.experience_key = '{e}'
    and current_cluster_search_terms.cluster_name = '{s}'
group by 1
having clicks > 0
order by clicks desc, searches desc
limit 10
"""

SOURCE_QUERY = """
select
    user_data.query_source,
    count(distinct searches.query_id) as searches,
    count(distinct user_data.session_id) as sessions,
    count(case when user_event_types.is_click_event then 1 end) as clicks,
    div0(count(distinct case when user_event_types.is_click_event then searches.query_id end), count(distinct searches.query_id)) as ctr
from
    searches
    left join user_data on searches.id = user_data.search_id
    left join user_events on searches.id = user_events.search_id
    left join user_event_types on user_events.user_event_type_id = user_event_types.id
where
    date(searches.timestamp) > dateadd('day', {d}, current_date())
    and user_data.traffic_source {t}
    and searches.version_label {l}
    and searches.business_id = {b}
    and searches.experience_key = '{e}'
    and searches.tokenizer_normalized_query = '{s}'
group by 1
having clicks > 0
order by clicks desc, searches desc
limit 10
"""

SOURCE_QUERY_C = """
select
    user_data.query_source,
    count(distinct searches.query_id) as searches,
    count(distinct user_data.session_id) as sessions,
    count(case when user_event_types.is_click_event then 1 end) as clicks,
    div0(count(distinct case when user_event_types.is_click_event then searches.query_id end), count(distinct searches.query_id)) as ctr
from
    searches
    join user_data on searches.id = user_data.search_id
    join current_cluster_search_terms on searches.business_id = current_cluster_search_terms.business_id and searches.experience_key = current_cluster_search_terms.experience_key and searches.tokenizer_normalized_query = current_cluster_search_terms.search_term
    left join user_events on searches.id = user_events.search_id
    left join user_event_types on user_events.user_event_type_id = user_event_types.id
where
    date(searches.timestamp) > dateadd('day', {d}, current_date())
    and user_data.traffic_source {t}
    and searches.version_label {l}
    and searches.business_id = {b}
    and searches.experience_key = '{e}'
    and current_cluster_search_terms.cluster_name = '{s}'
group by 1
having clicks > 0
order by clicks desc, searches desc
limit 10
"""

LOGS_QUERY = """
select
    searches.timestamp,
    searches.query_id,
    searches.tokenizer_normalized_query as query,
    concat(user_data.city, ', ', user_data.region) as city,
    user_data.country,
    concat(user_data.latitude, ', ', user_data.longitude) as "LAT, LONG"
from searches
left join user_data on searches.id = user_data.search_id
where
    date(searches.timestamp) > dateadd('day', {d}, current_date())
    and user_data.traffic_source {t}
    and searches.version_label {l}
    and searches.business_id = {b}
    and searches.experience_key = '{e}'
    and searches.tokenizer_normalized_query = '{s}'
order by 1 desc
limit 10
"""

LOGS_QUERY_C = """
select
    searches.timestamp,
    searches.query_id,
    searches.tokenizer_normalized_query as query,
    concat(user_data.city, ', ', user_data.region) as city,
    user_data.country,
    concat(user_data.latitude, ', ', user_data.longitude) as "LAT, LONG"
from
    searches
    join user_data on searches.id = user_data.search_id
    join current_cluster_search_terms on searches.business_id = current_cluster_search_terms.business_id and searches.experience_key = current_cluster_search_terms.experience_key and searches.tokenizer_normalized_query = current_cluster_search_terms.search_term
where
    date(searches.timestamp) > dateadd('day', {d}, current_date())
    and user_data.traffic_source {t}
    and searches.version_label {l}
    and searches.business_id = {b}
    and searches.experience_key = '{e}'
    and current_cluster_search_terms.cluster_name = '{s}'
order by 1 desc
limit 10
"""

PARAMS = {
    "popular_query": {
        "Search Term": POPULAR_SEARCH_TERMS,
        "Cluster": POPULAR_CLUSTERS,
    },
    "popular_query_col": {
        "Search Term": "TOKENIZER_NORMALIZED_QUERY",
        "Cluster": "CLUSTER_NAME",
    },
    "head": {"Search Term": "Search Terms", "Cluster": "Search Term Clusters"},
    "url": {
        "Search Term": "https://www.yext.com/s/{}/answers/experiences/{}/uniqueQueries",
        "Cluster": "https://www.yext.com/s/{}/answers/experiences/{}/clusters",
    },
    "analytics_query": {
        "Search Term": ANALYTICS_QUERY,
        "Cluster": ANALYTICS_QUERY_C,
    },
    "cluster_query": {
        "Search Term": CLUSTER_QUERY,
        "Cluster": CLUSTER_QUERY_C,
    },
    "results_query": {
        "Search Term": RESULTS_QUERY,
        "Cluster": RESULTS_QUERY_C,
    },
    "vertical_query": {
        "Search Term": VERTICALS_QUERY,
        "Cluster": VERTICALS_QUERY_C,
    },
    "source_query": {
        "Search Term": SOURCE_QUERY,
        "Cluster": SOURCE_QUERY_C,
    },
    "logs_query": {
        "Search Term": LOGS_QUERY,
        "Cluster": LOGS_QUERY_C,
    },
}
