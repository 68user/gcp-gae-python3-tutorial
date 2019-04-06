#!/usr/bin/python3
from flask import Flask
from google.cloud import bigquery

app = Flask(__name__)
bigquery_client = bigquery.Client()

                    
@app.route('/')
def do_main():
    return """
<p>GCP の各サービスと連携するサンプルです。</p>
<ul>
   <li><a href="/bqquery">BigQuery</a></li>
</ul>
"""


@app.route('/bqquery')
def do_bqquery():
    from flask import render_template_string

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
<table style="  border-collapse: collapse">
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

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
