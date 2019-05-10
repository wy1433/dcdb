# from flask.globals import request
from flask import Flask, session, redirect, url_for, escape, request
import json
from sql.server.server import Server
from mylog import logger

server = Server()

app = Flask(__name__)

# curl -X POST -d  'sql=select * from t'  'http://localhost:9000/'
@app.route('/', methods=['POST'])
def index():
    if request.method == 'POST':
        if 'sql' not in request.form:
            return 'no sql\n'
        
        ret = login()
        session_id = session.get('session_id')
        if not session_id:
            return ret
        
        sql = request.form['sql']
        ret = server.Run(sql, session_id)
        return ret
#         return 'sid=%d, sql=%s\n' % (session_id, sql)
    

@app.route('/login', methods=['GET', 'POST'])
def login():
    if 'session_id' in session:
        ret = 'Already Logged in, session_id=%d\n' % session['session_id']
        logger.debug(ret)
        return ret
    else:
        session_id, err = server.login() #: :type err: ErrSessionMaxSize
        if err:
            ret = 'Logged in err:%s\n' % err.ERROR()
            logger.debug(ret)
            return  ret
        else:
            session['session_id'] = session_id
            ret = 'Logged in succ, session_id=%d\n' % session_id
            return ret
        

@app.route('/logout')
def logout():
    # remove the session_id from the session if it's there
    session_id = 0
    if 'session_id' in session:
        session_id = session.get('session_id')
        session.pop('session_id')
        server.logout(session_id)
        
    ret = 'Logged out session_id=%d\n' % session_id
    logger.debug(ret)
    return ret

# set the secret key.  keep this really secret:
app.secret_key = '\x13p\xda\xc8\xb9.l\x7f\x19+\xfe\x93\x94\xa5\x1c\x8d\xf0\xe0;Sg\x86\xf4\xcb'


if __name__ == '__main__':
#     app.debug = True
    app.run(host='0.0.0.0',port=9000)