import json,subprocess,select,hashlib,time,hmac,psycopg2,threading

def main():

    f = subprocess.Popen(['tail','-F',"-n+1","/data/logs/kong/file.json"], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    p = select.poll()
    p.register(f.stdout)
    global i,sum,count

    while True:
        if p.poll(1):
            s=str(f.stdout.readline())
            s=s[2:-3]
            t = time.time()
            parse(s)
            t1=time.time()-t
            sum+=t1
            print(sum/count)


def parse(log_line):
    formatted=""
    data = {}

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

        file = open("/data/logs/kong/kong.log", "a")
        file.write(formatted)
        file.write("\n")

        addHash(formatted)

def addHash(parsed_line):

    global i,rows,tup,last_hash,count

    temp_row = ()
    temp_row += (parsed_line,)
    temp = parsed_line.split()
    logLine=""

    prev_hash = ""
    key=""

    if i==0:
        hash_object = hmac.new(b'wO5DWccVT8+vpSDKCPPN_NU4#3sU_Y8Vs!b9ftY|8iymQXphl_yf*yV72@AWpV$&*=67LTltScUS!v9Df|QYEG6X78C2a0d|?&i9WS4kgn1x1b4XBpJWuwR?bhGBu1yn',"smartcity".encode("UTF-8"),digestmod=hashlib.sha512)
        hex_dig = hash_object.hexdigest()
        temp.insert(3,hex_dig)
    else:
        temp.insert(3, last_hash)

    logLine = " ".join(temp)

    hash_object=hmac.new(b'wO5DWccVT8+vpSDKCPPN_NU4#3sU_Y8Vs!b9ftY|8iymQXphl_yf*yV72@AWpV$&*=67LTltScUS!v9Df|QYEG6X78C2a0d|?&i9WS4kgn1x1b4XBpJWuwR?bhGBu1yn',logLine.encode("UTF-8"),digestmod=hashlib.sha512)
    hex_dig = hash_object.hexdigest()
    temp_row += (hex_dig,)
    last_hash = hex_dig
    temp.insert(len(temp),hex_dig)
    logLine=" ".join(temp)

    print(logLine)

    rows.append(temp_row)
    tup += (temp_row,)

    i+=1
    count+=1

    if(i==1000):
        i=0
        thr = threading.Thread(target=batch_insert, args=([tup]), kwargs={})
        thr.start()
        rows[:]=[]
        tup = ()

def batch_insert(row_list):
    conn = psycopg2.connect(database="postgres", user="postgres", password="password", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    try:

        cur.executemany("insert into logs(logline,hash) values(%s,%s)", row_list)
        conn.commit()
        conn.close()
    except psycopg2.DatabaseError as e:
        print(e)


i = 0
rows=[]
tup=()
last_hash=""
sum=0
count=0

if __name__ == '__main__':
    main()
