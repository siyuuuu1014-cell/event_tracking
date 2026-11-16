import json
from datetime import UTC, timedelta
from itertools import groupby, islice
from uuid import UUID

INQUIRY_DATASET = "inquiryCreated"
FLOW_DATASET = "inquiryFlows"
CHAT_DATASET = "dialogReplies"
WINDOW = timedelta(hours=24)

SOURCE_EVENTS = """
SELECT business_date,actor_key,occurred_at,event_name,page_view_id,properties_json
FROM
(
    SELECT * FROM events_raw
    WHERE business_date >= subtractDays({fromDate:Date},1)
      AND business_date < addDays({toDate:Date},1)
      AND occurred_at <= {watermark:DateTime64(3, 'UTC')}
      AND ingested_at <= {watermark:DateTime64(3, 'UTC')}
      AND arrayExists(c -> c.1=topic AND c.2=kafka_partition AND kafka_offset<=c.3,
            {watermarks:Array(Tuple(String,UInt32,UInt64))})
      AND identity_type='authenticated' AND actor_key IS NOT NULL
      AND ((source='app' AND event_name IN {appEvents:Array(String)})
           OR (source='server' AND event_name={businessEvent:String}))
    ORDER BY received_at,topic,kafka_partition,kafka_offset,ingested_at,payload_hash
    LIMIT 1 BY event_id
)
"""

CHAT_QUERY = SOURCE_EVENTS + """
WHERE JSONExtractString(properties_json,'senderKind')='human'
  AND JSONExtractString(properties_json,'messageType')!='card'
ORDER BY JSONExtractString(properties_json,'dialogRef'),occurred_at,event_id
LIMIT 1 BY JSONExtractString(properties_json,'dialogRef'),JSONExtractString(properties_json,'messageRef')
"""

INQUIRY_QUERY = SOURCE_EVENTS + " ORDER BY actor_key,occurred_at,ifNull(sequence,0),event_id"


def append_batches(ch, table, columns, rows):
    while batch := list(islice(rows, 1000)):
        ch.insert(table,batch,column_names=columns)


def reply_rows(rows, generation, start, end, watermark):
    anchor = None
    for day,actor,when,_,_,props in rows:
        if anchor is None:
            anchor = (day,actor,when,props)
            continue
        first_day,sender,started,first = anchor
        if when-started > WINDOW or actor!=sender:
            replied = when if when-started<=WINDOW and actor!=sender else None
            if start<=first_day<end:
                yield reply_row(generation,first_day,sender,started,first,replied,watermark)
            close = replied is not None
            anchor = None if close else (day,actor,when,props)
    if anchor is not None:
        day,sender,started,props = anchor
        if start<=day<end:
            yield reply_row(generation,day,sender,started,props,None,watermark)


def reply_row(generation,day,sender,started,props,replied,watermark):
    latency = int((replied-started).total_seconds()*1000) if replied else None
    return (generation,day,props["dialogRef"],props["messageRef"],sender,started,replied,latency,
            watermark>=started+WINDOW)


def inquiry_rows(rows,generation,start,end):
    details,flows,matched = {},{},set()
    inquiries = []
    for day,actor,when,event,page,props in rows:
        company,product = props["companyId"],props.get("productId")
        key = (page,company,product)
        flow = props.get("contactFlowId")
        if event=="entity_detail_view":
            if page:
                details[key] = when
            continue
        if event=="contact_supplier_click":
            detail = details.get(key)
            if flow and detail is not None and when-detail<=WINDOW:
                flows.setdefault(flow,{"day":day,"actor":actor,"page":page,"company":company,"product":product,
                                       "detail":detail,"contact":when})
            continue
        match = flows.get(flow)
        if match and (when-match["detail"]>WINDOW or (match["company"],match["product"])!=(company,product)):
            match = None
        request = props.get("clientRequestId")
        if event=="inquiry_submit":
            if match and request and "request" not in match:
                match.update(request=request,submit=when)
            continue
        ref = props["inquiryRef"]
        if ref in matched:
            continue
        matched.add(ref)
        reason = None
        if not match:
            reason = "missingFlow"
        elif not request or match.get("request")!=request or "inquiry" in match:
            reason = "unmatchedRequest"
        else:
            match.update(inquiry=ref,created=when)
        if start<=day<end:
            inquiries.append((generation,day,ref,actor,when,company,product,UUID(flow) if reason is None else None,reason))
    flow_rows = (
        (generation,f["day"],f["actor"],f["page"],UUID(flow),f["company"],f["product"],f["detail"],f["contact"],
         f.get("request"),f.get("submit"),f.get("inquiry"),f.get("created"),
         "matched" if "inquiry" in f else "missingCreated" if "submit" in f else "missingSubmit")
        for flow,f in flows.items() if start<=f["day"]<end
    )
    return inquiries,flow_rows


def read_rows(stream, tz):
    for block in stream:
        for day,actor,at,event,page,props in block:
            yield day,actor,at if at.tzinfo else at.replace(tzinfo=tz),event,page,json.loads(props)


def build_business(ch, writer, parameters):
    generation,start,end = (parameters[key] for key in ("generation","fromDate","toDate"))
    # Use a second ClickHouse client because an open result stream holds its connection.
    with ch.query_row_block_stream(CHAT_QUERY,parameters={
        **parameters,"appEvents":[],"businessEvent":"message_send_succeeded",
    },settings={"max_execution_time":60}) as stream:
        rows = read_rows(stream,UTC)
        replies = (reply for _,thread in groupby(rows,key=lambda row:row[5]["dialogRef"])
                   for reply in reply_rows(thread,generation,start,end,parameters["watermark"]))
        append_batches(writer,"fact_dialog_reply",
            ("generation","business_date","dialog_ref","anchor_message_ref","sender_actor_key",
             "anchor_at","reply_at","latency_ms","mature"),replies)

    with ch.query_row_block_stream(INQUIRY_QUERY,parameters={
        **parameters,"appEvents":["entity_detail_view","contact_supplier_click","inquiry_submit"],
        "businessEvent":"inquiry_created",
    },settings={"max_execution_time":60}) as stream:
        inquiry_batch, flow_batch = [], []
        inquiry_columns = ("generation","business_date","inquiry_ref","actor_key","submitted_at","company_id",
                           "product_id","contact_flow_id","attribution_reason")
        flow_columns = ("generation","business_date","actor_key","page_view_id","contact_flow_id","company_id","product_id",
                        "detail_at","contact_at","client_request_id","submit_at","inquiry_ref","created_at","attribution_status")
        for _,actor in groupby(read_rows(stream,UTC),key=lambda row:row[1]):
            inquiries,flows = inquiry_rows(actor,generation,start,end)
            for batch,new_rows,table,columns in (
                (inquiry_batch,inquiries,"fact_inquiry_created",inquiry_columns),
                (flow_batch,flows,"fact_inquiry_flow",flow_columns),
            ):
                for row in new_rows:
                    batch.append(row)
                    if len(batch)==1000:
                        writer.insert(table,batch,column_names=columns)
                        batch.clear()
        if inquiry_batch:
            writer.insert("fact_inquiry_created",inquiry_batch,column_names=inquiry_columns)
        if flow_batch:
            writer.insert("fact_inquiry_flow",flow_batch,column_names=flow_columns)

def chat_reply_metrics(ch,pairs,anchor_refs):
    from ipaklink_analytics.application.event_reports import metric, ratio

    row = ch.query(
        """SELECT countIf(mature),countIf(mature AND reply_at IS NOT NULL),
                  uniqExactIf(sender_actor_key,mature),count(latency_ms),
                  quantileExactInclusiveIf(0.5)(assumeNotNull(latency_ms),latency_ms IS NOT NULL)
           FROM fact_dialog_reply
           WHERE (business_date,generation) IN {pairs:Array(Tuple(Date,UInt64))}
             AND (dialog_ref,anchor_message_ref) IN {anchors:Array(Tuple(String,String))}""",
        parameters={"pairs":pairs,"anchors":anchor_refs},settings={"max_execution_time":5},
    ).first_row
    total,replied,actors,timed,latency = row
    return [
        metric("chatReplyRate24h",ratio(replied,total),"ratio",small=0<actors<20,
               numerator=replied,denominator=total),
        metric("chatReplyP50Ms",latency if timed else None,"ms",small=0<actors<20),
    ]
