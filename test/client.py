# -*-coding:utf-8 -*-
import requests

if __name__ == '__main__':
    d={"sql":'select id, name, age from student where id > 0'}
    url='http://localhost:9000/'
    r = requests.post(url=url,data=d)
    print r.text