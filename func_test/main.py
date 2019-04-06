#!/usr/bin/python3
from flask import Flask
from google.cloud import bigquery
from flask_httpauth import HTTPBasicAuth

app = Flask(__name__)
bigquery_client = bigquery.Client()
auth = HTTPBasicAuth()

users = {
        "myuser": "mypass",
        "myuser2": "mypass2"
    }

@auth.get_password
def get_password(username):
    if username in users:
        return users.get(username)
    return None

@app.route('/authpage')
@auth.login_required
def authpage():
    return "{} でログイン中です!".format(escape(auth.username()))

                    
@app.route('/')
def do_main():
    return """
<p>これはメインページです</p>
<ul>
   <li><a href="/subpage">サブページへ</a></li>
   <li><a href="/img/sample.png">サンプル画像</a></li>
   <li><a href="/js/sample.js">サンプルjs</a></li>
   <li><a href="/error404">404 サンプル</a></li>
   <li><a href="/error503">503 サンプル</a></li>
   <li><a href="/getvar?text=hoge">GETパラメータ (text) 取得・表示</a></li>
   <li><a href="/pathvar/hoge/123">パスパラメータ (/hoge/123) 取得・表示</a></li>
   <li><a href="/bigquery">BigQuery</a></li>
   <li><a href="/logging">ログ出力</a></li>
   <li><a href="/authpage">Basic認証 (ID: myuser、パスワード: mypass)</a></li>
   <li><a href="/fileread">ファイル読み込み</a></li>
   <li><a href="/runcommand">ls -l と df コマンドを実行</a></li>
   <li><a href="/ls">ファイル一覧</a></li>
   <li><a href="/printcwd">カレントディレクトリ表示</a></li>
   <li><a href="/printipaddr">IP アドレス表示</a></li>
   <li><a href="/printenv">環境変数一覧</a></li>
   <li><a href="/printrequest">リクエスト</a></li>
   <li><a href="/ext_requests">外部コンテンツ取得</a></li>
</ul>
"""

@app.route('/getvar')
def do_getvar():
    from flask import request, escape
    mytext = request.args.get('text')
    return "text は [{}] です".format(escape(mytext))

@app.route('/pathvar/<pageId>/<pageNo>')
def do_pathvar(pageId=None, pageNo=None):
    from flask import escape
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

@app.route('/fileread')
def do_fileread():
    from flask import escape
    with open("read-sample.txt") as f:
        buf = f.read()
    return escape(buf)

@app.route('/logging')
def do_logging():
    logging.info('info です')
    logging.debug('debug です')
    return "logging OK"

@app.route('/bigquery')
def bqdatasets():
    from flask import render_template_string
    import concurrent.futures
    
    query_job = bigquery_client.query("""
xselect * FROM
 `bigquery-public-data.INFORMATION_SCHEMA`.SCHEMATA
 order by schema_name
    """)

    try:
        results = query_job.result(timeout=30)
    except concurrent.futures.TimeoutError:
        return "{} がタイムアウト".format(query_job.job_id)

    return render_template_string("""
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
    """, results = results)

@app.route('/runcommand')
def do_runcommand():
    from flask import render_template_string
    import subprocess

    cmd1 = "/bin/ls -l /bin hogehoge"
    proc1 = subprocess.run(cmd1.split(' '), stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    rc1 = proc1.returncode
    stdout1 = proc1.stdout.decode("utf8")
    stderr1 = proc1.stderr.decode("utf8")

    cmd2 = "/bin/df"
    proc2 = subprocess.run(cmd2.split(' '), stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    rc2 = proc2.returncode
    stdout2 = proc2.stdout.decode("utf8")
    stderr2 = proc2.stderr.decode("utf8")

    return render_template_string("""
<p>{{ cmd1 }}</p>
returncode: {{ rc1 }}<br>
stdout: <pre style="border: 1px solid black">{{ stdout1 }}</pre>
stderr: <pre style="border: 1px solid black">{{ stderr1 }}</pre>

<p>{{ cmd2 }}</p>
returncode: {{ rc2 }}<br>
stdout: <pre style="border: 1px solid black">{{ stdout2 }}</pre>
stderr: <pre style="border: 1px solid black">{{ stderr2 }}</pre>
    """,
                                  cmd1=cmd1, rc1=rc1, stdout1=stdout1, stderr1=stderr1,
                                  cmd2=cmd2, rc2=rc2, stdout2=stdout2, stderr2=stderr2)


@app.route('/printenv')
def do_printenv():
    from flask import render_template_string
    import os

    return render_template_string("""
<ul>
 {% for k,v in envs %}
   <li>{{ k }}: {{ v }}</li>
 {% endfor %}  
</ul>
    """, envs = sorted(os.environ.items()))

@app.route('/printcwd')
def do_printcwd():
    import os
    buf = "カレントディレクトリは [{}] です。".format(os.getcwd())
    return buf


@app.route('/printipaddr')
def do_printipaddr():
    from flask import request
    from flask import render_template_string
    return render_template_string("""
    IPアドレスは [{{remote_addr}}]<br>
    Forwarded は [{{forwarded}}]<br>
    X-Appengine-User-Ip は [{{user_ip}}]<br>
    X-Forwarded-For は [{{forwarded_for}}]<br>
    """,
                                  remote_addr = request.remote_addr,
                                  forwarded = request.headers.get('Forwarded'),
                                  user_ip = request.headers.get('X-Appengine-User-Ip'),
                                  forwarded_for = request.headers.get('X-Forwarded-For'))

@app.route('/printrequest')
def do_printrequest():
    from flask import request, escape
    buf = ''
    buf = request.remote_addr
    for k,v in sorted(request.headers):
        buf += "{}: {}<br>\n".format(escape(k), escape(v))
    return buf

@app.route('/ext_requests')
def do_ext_requests():
    import requests
    from flask import render_template_string
    
    url = 'https://www.yahoo.co.jp/'
    res = requests.get(url)

    return render_template_string("""
<p>url: {{ url }}</p>
<p>status_code: {{ status_code }}</p>
<p>===== response header ======</p>
<ul>
  {% for k,v in headers.items() %}
    <li>{{ k }}: {{ v }}</li>
  {% endfor %}
</ul>
<p>===== response body ======</p>
<pre>{{ body }}</pre>
""",
                                  url=res.url,
                                  status_code = res.status_code,
                                  headers = res.headers,
                                  body=res.text
    )

@app.route('/cat')
def do_cat():
    path = request.args.get('path')
    with open(path) as f:
        buf = f.read()
    return "<pre>"+buf+"</pre>"


@app.route('/ls')
def do_ls():
    from flask import request
    import os
    
    S_IFSOCK=0o0140000 # ソケット
    S_IFLNK=0o0120000  # シンボリックリンク
    S_IFREG=0o0100000  # 通常のファイル
    S_IFBLK=0o0060000  # ブロックデバイス
    S_IFDIR=0o0040000  # ディレクトリ
    S_IFCHR=0o0020000  # キャラクターデバイス
    S_IFIFO=0o0010000  # FIFO

    S_IRUSR    = 0o00400
    S_IWUSR    = 0o00200
    S_IXUSR    = 0o00100

    S_IRGRP    = 0o00040
    S_IWGRP    = 0o00020
    S_IXGRP    = 0o00010

    S_IROTH    = 0o00004
    S_IWOTH    = 0o00002
    S_IXOTH    = 0o00001
    
    path = request.args.get('path')
    if path == '' or path is None:
        path = '/'

    buf = ''
    files = os.listdir(path)
    for file in sorted(files):
        statinfo = os.stat(path+"/"+file)
        mode = statinfo.st_mode & 0o0170000
        if mode == S_IFSOCK:
            type = 's'
        elif mode == S_IFLNK:
            type = 'l'
        elif mode == S_IFREG:
            type = '-'
        elif mode == S_IFBLK:
            type = 'b'
        elif mode == S_IFDIR:
            type = 'd'
        elif mode == S_IFCHR:
            type = 'c'
        elif mode == S_IFIFO:
            type = 'f'
        else:
            type = '?'
        if type == 'd':
            link = "<a href='/ls?path={}/{}'>{}</a>".format(path, file, file)
        elif type == '-':
            link = "<a href='/cat?path={}/{}'>{}</a>".format(path, file, file)
        else:
            line = file
        buf += "{}{}{}{}{}{}{}{}{}{} {} {} {} {} {}<br>\n".format(type,
                                                'r' if statinfo.st_mode & S_IRUSR > 0 else '-',
                                                'w' if statinfo.st_mode & S_IWUSR > 0 else '-',
                                                'x' if statinfo.st_mode & S_IXUSR > 0 else '-',
                                                'r' if statinfo.st_mode & S_IRGRP > 0 else '-',
                                                'w' if statinfo.st_mode & S_IWGRP > 0 else '-',
                                                'x' if statinfo.st_mode & S_IXGRP > 0 else '-',
                                                'r' if statinfo.st_mode & S_IROTH > 0 else '-',
                                                'w' if statinfo.st_mode & S_IWOTH > 0 else '-',
                                                'x' if statinfo.st_mode & S_IXOTH > 0 else '-',
                                                statinfo.st_uid, statinfo.st_gid, statinfo.st_size, statinfo.st_mtime,
                                                link)
    return buf

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
