import json
import subprocess
import select
import hashlib
import time
import hmac
import psycopg2
import sys


def main():
    f = subprocess.Popen(['tail','-F',"-n+1","/data/logs/kong/file.json"], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    p = select.poll()
    p.register(f.stdout)
    global i

    while True:
        if p.poll(1):
            s=str(f.stdout.readline())
            s=s[2:-3]
            i+=1
            t = time.time()
            parse(s)
            t1=time.time()-t
            print(t1)

def parse(log_line):

    formatted=""
    data=dict()

    try:
        data=json.loads(log_line)
    except:
        pass

    if len(data)!=0:

        ip = str(data['client_ip'])

        start_time = data['started_at'] / 1000.0
        timestamp = time.strftime('%d-%m-%Y %H:%M:%S', time.localtime(start_time))
        formatted += timestamp + " " + ip + " "

        request_method = str(data['request']['method'])

        temp = str(data['request']['uri']).split("\\/")

        uri=[""]

        try:
            uri = temp[3].split("?")
        except:
            pass


        if (str(uri[0]) == "subscribe" and len(temp) > 3 and len(uri) == 1):
            uri[0] += "Bind"

        formatted += request_method + " " + uri[0] + " "
        resourceId = ""

        try:
            resourceId = str(data['request']['headers']['resourceid'])
        except Exception as e:
            pass

        username = ""
        consumerId = ""

        apikey=""

        try:
            apikey = str(data['request']['headers']['apikey'])
        except:
            pass

        try:
            username = str(data['request']['headers']['x-consumer-username'])
            consumerId = str(data['request']['headers']['x-consumer-id'])
        except Exception as e:
            pass

        response=""
        try:
            response = str(data['response']['status'])
        except:
            pass

        formatted += resourceId + " " + apikey + " " + username + " " + consumerId + " " + response
        formatted=" ".join(formatted.split())

        addHash(formatted)

def addHash(parsed_line):

    global i
    global rows
    temp_row=parsed_line+"~"
    temp = parsed_line.split()
    logLine=""

    prev_hash = ""
    key=""

    if i==1:
        hash_object = hmac.new(b'testkey',"smartcity".encode("UTF-8"),digestmod=hashlib.sha512)
        hex_dig = hash_object.hexdigest()
        temp.insert(3,hex_dig)
    else:
        f = subprocess.Popen(['tail', '-1', "/data/logs/kong/kong.log"], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        p = select.poll()
        p.register(f.stdout)

        if p.poll(1):
            prev_logline = str(f.stdout.readline()).split()
            prev_hash=prev_logline[-1].replace("\\n","")[:-1]
            temp.insert(3, prev_hash)

        logLine = " ".join(temp)

    hash_object=hmac.new(b'testkey',logLine.encode("UTF-8"),digestmod=hashlib.sha512)
    hex_dig = hash_object.hexdigest()
    temp_row+=hex_dig
    temp.insert(len(temp),hex_dig)
    logLine=" ".join(temp)

    print(logLine)

    file=open("/data/logs/kong/kong.log","a")
    file.write(logLine)
    file.write("\n")

    rows.append(temp_row)

    if(i==100):
        batch_insert()

def batch_insert():
    global rows
    global i
    i = 0
    conn = psycopg2.connect(database="postgres", user="postgres", password="password", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    try:
        for row in rows:
            log=row.split("~")
            cur.execute("insert into logs(logline,hash) values(%s,%s)",(log[0],log[1]))

        conn.commit()
        conn.close()
        rows[:]=[]
    except psycopg2.DatabaseError as e:
        print(e)



i = 0
rows=[]
main()