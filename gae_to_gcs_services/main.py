#!/usr/bin/python3
from flask import Flask

app = Flask(__name__)

                    
@app.route('/')
def do_main():
    return """
<p>GCP の各サービスと連携するサンプルです。</p>
<ul>
   <li><a href="/bqquery">BigQuery</a></li>
   <li><a href="/storage">Cloud Storage (GCS)</a></li>
   <li><a href="/pubsub_publish">Cloud Pub/Sub へ Publish</a></li>
   <li><a href="/pubsub_pull">Cloud Pub/Sub から Pull</a></li>
</ul>
"""


@app.route('/bqquery')
def do_bqquery():
    from flask import render_template_string
    from google.cloud import bigquery

    bigquery_client = bigquery.Client()

    sql = """
#StandardSQL
select * FROM
 `bigquery-public-data.INFORMATION_SCHEMA`.SCHEMATA
 order by schema_name
"""
    
    query_job = bigquery_client.query(sql)
    try:
        results = query_job.result()
    except:
        return "errorResult {}".format(query_job.error_result)

    # $5 per 1TB
    cost_usd = "%f" % (5 * query_job.total_bytes_billed / (1024*1024*1024*1024))
    # $1 = 110 JPY
    cost_jpy = "%f" % (110 * 5 * query_job.total_bytes_billed / (1024*1024*1024*1024))

    return render_template_string("""
<ul>
  <li>SQL<pre>{{ sql }}</pre></li>
  <li>job_id: {{ job_id }}</li>
  <li>total_bytes_billed (課金バイト数): {{ total_bytes_billed }}</li>
  <li>cost: ${{ cost_usd }} ({{ cost_jpy }}円)</li>
</ul>
<style type="text/css">
table th, table td {
  border: 1px solid black;
  font-size: 70%;
}
</style>
<table style="border-collapse: collapse">
  <thead>
    <th>catalog_name</th>
    <th>schema_name</th>
    <th>location</th>
    <th>creation_time</th>
    <th>last_modified_time</th>
  </thead>
  <tbody>
    {% for row in results %}
      <tr>
        <td>{{ row.catalog_name }}</td>
        <td>{{ row.schema_name }}</td>
        <td>{{ row.location }}</td>
        <td>{{ row.creation_time }}</td>
        <td>{{ row.last_modified_time }}</td>
      </tr>
    {% endfor  %}
  </tbody>
</table>
    """,
               sql = sql,
               total_bytes_billed = query_job.total_bytes_billed,
               job_id = query_job.job_id,
               cost_usd = cost_usd,
               cost_jpy = cost_jpy,
               results = results)


@app.route('/storage')
def do_storage():
    from flask import render_template_string
    from google.cloud import storage

    storage_client = storage.Client()
    bucket = storage_client.get_bucket('gcp-public-data-landsat')

    blob_list = bucket.list_blobs(prefix='LC08/PRE/044/034/LC80440342016259LGN00/',
                                  max_results=5)

    blob = bucket.get_blob('LC08/PRE/044/034/LC80440342016259LGN00/'
                           'LC80440342016259LGN00_MTL.txt')
    content = blob.download_as_string().decode(encoding='ASCII')

    return render_template_string("""
<p>オブジェクト一覧</p>
<ul>
  {% for b in blob_list %}
    <li>
      サイズ: {{ b.size }}
      オブジェクト名: {{ b.name }}
    </li>
  {% endfor %}
</ul>
<p>オブジェクト</p>
<ul>
  <li>バケット: {{ bucket }}</li>
  <li>オブジェクト名: {{ name }}</li>
  <li>サイズ: {{ size }}</li>
  <li>オブジェクト内容: <pre>{{ content }}</pre></li>
</ul>
    """,
                                  blob_list = blob_list,
                                  bucket = blob.bucket.name,
                                  name = blob.name,
                                  size = blob.size,
                                  content = content)

@app.route('/pubsub_publish')
def do_pubsub_publish():
    from flask import render_template_string
    from google.cloud import pubsub_v1
    import os

    topic_name='mytopic'
    project_id=os.environ.get('GOOGLE_CLOUD_PROJECT')

    topic_path = 'projects/{project_id}/topics/{topic_name}'.format(
        project_id=project_id,
        topic_name=topic_name,
    )
    publisher = pubsub_v1.PublisherClient()

    from datetime import datetime
    message = "current time is {}".format(datetime.now().strftime("%Y/%m/%d %H:%M:%S"))

    future = publisher.publish(topic_path, message.encode('utf-8'), attr1='abc', attr2='def')
    message_id = future.result()

    return render_template_string("""
<p>Pub/Sub message published.
<ul>
  <li>topic [{{ topic_path }}]</li>
  <li>message is [{{ message }}]</li>
  <li>message_id is [{{ message_id }}]</li>
</ul>
        """,
                                  topic_path = topic_path,
                                  message = message,
                                  message_id = message_id,
    )

@app.route('/pubsub_pull')
def do_pubsub_pull():
    from flask import render_template_string
    from google.cloud import pubsub_v1
    import os

    topic_name='mytopic'
    sub_name='mysub'

    project_id=os.environ.get('GOOGLE_CLOUD_PROJECT')
    sub_path = 'projects/{project_id}/subscriptions/{subscription}'.format(
        project_id=project_id,
        subscription=sub_name
    )

    subscriber = pubsub_v1.SubscriberClient()

    response = subscriber.pull(sub_path, max_messages=1, return_immediately=True)
    if len(response.received_messages) == 0:
        return "No messages"

    msg = response.received_messages[0]
    # ここで何らかの処理をする (DB に格納する、ファイルを生成するなど)
    subscriber.acknowledge(sub_path, [msg.ack_id])

    return render_template_string("""
<p>Pub/Sub message pulled and acked.
<ul>
  <li>subscription [{{ sub_path }}]</li>
  <li>message is [{{ message }}]</li>
  <li>ack_id is [{{ ack_id }}]</li>
</ul>
        """,
                                  sub_path = sub_path,
                                  message = msg.message,
                                  ack_id = msg.ack_id,
    )

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
