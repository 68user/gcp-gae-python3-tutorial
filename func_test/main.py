#!/usr/bin/python3
from flask import Flask, request, escape
from google.cloud import bigquery
from flask_httpauth import HTTPBasicAuth

app = Flask(__name__)
bigquery_client = bigquery.Client()
auth = HTTPBasicAuth()

users = {
        "john": "hello",
        "susan": "bye"
    }

@auth.get_password
def get_password(username):
    if username in users:
        return users.get(username)
    return None

@app.route('/authpage')
@auth.login_required
def authpage():
    return "%s でログイン中です!".format(auth.username())

                    
@app.route('/')
def do_main():
    return """
<p>これはメインページです</p>
<ul>
   <li><a href="/subpage">サブページへ</a></li>
   <li><a href="/img/sample.png">サンプル画像</li>
   <li><a href="/js/sample.js">サンプルjs</li>
   <li><a href="/error404">404 サンプル</li>
   <li><a href="/error503">503 サンプル</li>
   <li><a href="/getvar?text=hoge">GETパラメータ (text) 取得・表示</li>
   <li><a href="/pathvar/hoge/123">パスパラメータ (/hoge/123) 取得・表示</li>
   <li><a href="/bigquery">BigQuery</li>
</ul>
"""

@app.route('/getvar')
def do_getvar():
    mytext = request.args.get('text')
    return "text は [{}] です".format(escape(mytext))

@app.route('/pathvar/<pageId>/<pageNo>')
def do_pathvar(pageId=None, pageNo=None):
    return "pageId は [{}]、pageNo は [{}] です".format(escape(pageId), escape(pageNo))

@app.route('/subpage')
def do_subpage():
    return 'サブページです'

@app.route('/error404')
def do_error404():
    return ('404 です', 404)

@app.route('/error503')
def do_error503():
    return ('503 です', 503)


@app.route('/bigquery')
def bqdatasets():
    query_job = bigquery_client.query("""
select * FROM
 `bigquery-public-data.INFORMATION_SCHEMA`.SCHEMATA
 order by schema_name
    """)

    try:
        results = query_job.result(timeout=30)
    except concurrent.futures.TimeoutError:
        return "{} がタイムアウト".format(query_job.job_id)

    buf = ''
    for row in results:
        for k,v in row.items():
            buf+="{}:{}, ".format(k,v)
        buf += "<br>\n"
    return buf

                         

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
