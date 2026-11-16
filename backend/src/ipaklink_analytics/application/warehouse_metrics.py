from ipaklink_analytics.application.event_reports import metric, ratio

METRIC_DATASET = "dailyMetrics"

DAILY_INSERT = """
INSERT INTO agg_metric_day
    (generation,business_date,metric_code,metric_version,dim_set,dim_values,
     value_count,numerator,denominator,provisional)
WITH active AS
(
    SELECT business_date,uniqExact(actor_key) AS dau,uniqExactIf(actor_key,identity_type='anonymous') AS anonymous
    FROM fact_actor_active_day WHERE generation={generation:UInt64} GROUP BY business_date
),
translations AS
(
    SELECT business_date,countIf(outcome='success') AS succeeded,
           countIf(outcome IN ('success','failed')) AS finished
    FROM fact_translate_task WHERE generation={generation:UInt64} GROUP BY business_date
),
days AS
(
    SELECT addDays({fromDate:Date},number) AS date
    FROM numbers(toUInt64(dateDiff('day',{fromDate:Date},{toDate:Date})))
),
daily AS
(
    SELECT date,arrayJoin([
        tuple('dau',toNullable(dau),CAST(NULL,'Nullable(UInt64)'),CAST(NULL,'Nullable(UInt64)')),
        tuple('anonymousUv',toNullable(anonymous),CAST(NULL,'Nullable(UInt64)'),CAST(NULL,'Nullable(UInt64)')),
        tuple('translateSuccessRate',CAST(NULL,'Nullable(UInt64)'),toNullable(succeeded),toNullable(finished))
    ]) AS m
    FROM days LEFT JOIN active ON date=active.business_date
         LEFT JOIN translations ON date=translations.business_date
)
SELECT {generation:UInt64},date,m.1,toUInt32(1),'all','{}',m.2,m.3,m.4,date>={asOf:Date}
FROM daily SETTINGS max_execution_time=60, join_use_nulls=0
"""

DIMENSIONS = {
    "platform": ("platform", "String"),
    "regionId": ("region_id", "UInt32"),
    "allowedRegions": ("region_id", "Array(UInt32)"),
    "appVersion": ("app_version", "String"),
    "scope": ("scope", "String"),
    "mode": ("mode", "String"),
    "sourceLang": ("source_lang", "String"),
    "targetLang": ("target_lang", "String"),
    "errorCode": ("error_code", "String"),
}
GROUP_EXPRESSIONS = {
    "scope": "scope",
    "appVersion": "app_version",
    "mode": "mode",
    "languagePair": "concat(source_lang,':',target_lang)",
    "errorCode": "error_code",
}
FEATURE_TABLES = {
    "search": "fact_search",
    "translate": "fact_translate_task",
}
WINDOW_CONDITION = "(business_date,generation) IN {pairs:Array(Tuple(Date,UInt64))}"
QUERY_SETTINGS = {"max_execution_time": 5, "max_result_rows": 20000, "result_overflow_mode": "throw"}


def conditions(pairs, filters):
    params = {"pairs":pairs}
    clauses = [WINDOW_CONDITION]
    for key,value in filters.items():
        if value is not None:
            column,typ = DIMENSIONS[key]
            operator = "IN" if key=="allowedRegions" else "="
            clauses.append(f"{column} {operator} " + "{" + key + ":" + typ + "}")
            params[key] = value
    return " AND ".join(clauses),params


AGGREGATES = {
    "search": """
        count() AS started,uniqExact(actor_key) AS actors,
        count(first_open_at) AS opened,countIf(outcome='failed') AS failed,
        countIf(outcome='success') AS succeeded,countIf(outcome='success' AND result_count=0) AS zero,
        countIf(outcome='success' AND result_count>0) AS with_results
    """,
    "translate": """
        count() AS started,uniqExact(actor_key) AS actors,
        countIf(outcome='success') AS succeeded,countIf(outcome IN ('success','failed')) AS finished,
        countIf(outcome='success' AND latency_ms IS NOT NULL) AS timed,
        quantileExactIf(0.95)(assumeNotNull(latency_ms),outcome='success' AND latency_ms IS NOT NULL) AS p95
    """,
}


def metrics_for_group(feature, row):
    small = 0 < row["actors"] < 20
    if feature=="search":
        return [
            metric("searchSubmitted",row["started"],small=small),
            metric("searchOpened",row["opened"],small=small),
            metric("searchFailed",row["failed"],small=small),
            metric("searchZeroResultRate",ratio(row["zero"],row["succeeded"]),"ratio",small=small,
                   numerator=row["zero"],denominator=row["succeeded"]),
            metric("searchOpenRate",ratio(row["opened"],row["with_results"]),"ratio",small=small,
                   numerator=row["opened"],denominator=row["with_results"]),
        ]
    return [
        metric("translateStarted",row["started"],small=small),
        metric("translateSuccessRate",ratio(row["succeeded"],row["finished"]),"ratio",small=small,
               numerator=row["succeeded"],denominator=row["finished"]),
        metric("translateP95Ms",row["p95"] if row["timed"] else None,"ms",small=small),
    ]


def query_feature(ch, pairs, feature, filters, group_by, page, size):
    where,params = conditions(pairs,filters)
    table,aggregates = FEATURE_TABLES[feature],AGGREGATES[feature]
    summary = next(ch.query(f"SELECT {aggregates} FROM {table} WHERE {where}",
                            parameters=params,settings=QUERY_SETTINGS).named_results())
    daily = ch.query(
        f"""SELECT business_date,{aggregates} FROM {table} WHERE {where}
            GROUP BY business_date ORDER BY business_date""",
        parameters=params,settings=QUERY_SETTINGS,
    ).named_results()
    series = [{"date":row["business_date"],**metric_row} for row in daily for metric_row in metrics_for_group(feature,row)]
    items = []
    if group_by:
        expression = GROUP_EXPRESSIONS[group_by]
        groups = ch.query(
            f"""SELECT {expression} AS key,{aggregates} FROM {table}
                WHERE {where} AND {expression} IS NOT NULL AND notEmpty({expression})
                GROUP BY key ORDER BY key""",
            parameters=params,settings=QUERY_SETTINGS,
        ).named_results()
        items = [{"groupKey":row["key"],**result} for row in groups
                 for result in sorted(metrics_for_group(feature,row),key=lambda m:m["metricCode"])]
    return {"summary":metrics_for_group(feature,summary),"series":series,
            "list":items[(page-1)*size:page*size],"total":len(items)}


def query_search_funnel(ch, pairs, filters):
    where,params = conditions(pairs,filters)
    has_results = "outcome='success' AND result_count>0"
    opened = f"{has_results} AND matched_target_type IN ('company','product') AND first_open_at IS NOT NULL"
    detail = f"{opened} AND detail_at IS NOT NULL"
    row = next(ch.query(
        f"""SELECT count() AS submitted,uniqExact(actor_key) AS a0,
                   countIf({has_results}) AS with_results,uniqExactIf(actor_key,{has_results}) AS a1,
                   countIf({opened}) AS opened_count,uniqExactIf(actor_key,{opened}) AS a2,
                   countIf({detail}) AS detail_count,uniqExactIf(actor_key,{detail}) AS a3,
                   countIf(outcome='success' AND result_count=0) AS zero,
                   countIf(outcome='failed') AS failed,
                   quantileExactInclusiveIf(0.5)(dateDiff('millisecond',submitted_at,assumeNotNull(first_result_at)),{has_results}) AS p1,
                   quantileExactInclusiveIf(0.5)(dateDiff('millisecond',assumeNotNull(first_result_at),assumeNotNull(first_open_at)),{opened}) AS p2,
                   quantileExactInclusiveIf(0.5)(dateDiff('millisecond',assumeNotNull(first_open_at),assumeNotNull(detail_at)),{detail}) AS p3
            FROM fact_search WHERE {where} AND scope!='user'""",
        parameters=params,settings=QUERY_SETTINGS,
    ).named_results())
    values = [row[k] for k in ("submitted","with_results","opened_count","detail_count")]
    steps = [{"stepCode":code,"count":values[index],"uniqueActors":row[f"a{index}"],
              "stepRate":ratio(values[index],values[max(index-1,0)]),
              "totalRate":ratio(values[index],values[0]),
              "p50FromPreviousMs":row[f"p{index}"] if index and values[index] else None}
             for index,code in enumerate(("submit","hasResults","open","detail"))]
    return {"funnelCode":"F02","windowSeconds":900,"entityUnit":"search","steps":steps,
            "businessCount":None,"attributedCount":None,"attributionCoverage":None,"attributionReasons":[],
            "zeroResultCount":row["zero"],"failedCount":row["failed"]}


def daily_overview(ch,pairs):
    return dict(((day,code),(value,top,bottom)) for day,code,value,top,bottom in ch.query(
        """SELECT business_date,metric_code,value_count,numerator,denominator FROM agg_metric_day
           WHERE (business_date,generation) IN {pairs:Array(Tuple(Date,UInt64))}
           AND dim_set='all' AND metric_version=1""",
        parameters={"pairs":pairs},settings=QUERY_SETTINGS,
    ).result_rows)
