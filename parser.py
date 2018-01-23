import json,subprocess,select,hashlib,time,hmac,psycopg2,threading
# import os
# import re
# from threading import Lock
#
# import psycopg2.extensions as ext
#
# class PreparingCursor(ext.cursor):
#     _lock = Lock()
#     _ncur = 0
#     def __init__(self, *args, **kwargs):
#         super(PreparingCursor, self).__init__(*args, **kwargs)
#         # create a distinct name for the statements prepared by this cursor
#         self._lock.acquire()
#         self._prepname = "psyco_%x" % self._ncur
#         PreparingCursor._ncur += 1
#         self._lock.release()
#
#         self._prepared = None
#         self._execstmt = None
#
#     _re_replargs = re.compile(r'(%s)|(%\([^)]+\)s)')
#
#     def prepare(self, stmt):
#         """Prepare a query for execution.
#         TODO: handle literal %s and $s in the string.
#         """
#         # replace the python placeholders with postgres placeholders
#         parlist = []
#         parmap = {}
#         parord = []
#
#         def repl(m):
#             par = m.group(1)
#             if par is not None:
#                 parlist.append(par)
#                 return "$%d" % len(parlist)
#             else:
#                 par = m.group(2)
#                 assert par
#                 idx = parmap.get(par)
#                 if idx is None:
#                     idx = parmap[par] = "$%d" % (len(parmap) + 1)
#                     parord.append(par)
#
#                 return idx
#
#         pgstmt = self._re_replargs.sub(repl, stmt)
#
#         if parlist and parmap:
#             raise psycopg2.ProgrammingError(
#                 "you can't mix positional and named placeholders")
#
#         self.deallocate()
#         self.execute("prepare %s as %s" % (self._prepname, pgstmt))
#
#         if parlist:
#             self._execstmt = "execute %s (%s)" % (
#                 self._prepname, ','.join(parlist))
#         elif parmap:
#             self._execstmt = "execute %s (%s)" % (
#                 self._prepname, ','.join(parord))
#         else:
#             self._execstmt = "execute %s" % (self._prepname)
#
#         self._prepared = stmt
#
#     @property
#     def prepared(self):
#         """The query currently prepared."""
#         return self._prepared
#
#     def deallocate(self):
#         """Deallocate the currently prepared statement."""
#         if self._prepared is not None:
#             self.execute("deallocate " + self._prepname)
#             self._prepared = None
#             self._execstmt = None
#
#     def execute(self, stmt=None, args=None):
#         if stmt is None or stmt == self._prepared:
#             stmt = self._execstmt
#         elif not isinstance(stmt, str):
#             args = stmt
#             stmt = self._execstmt
#
#         if stmt is None:
#             raise psycopg2.ProgrammingError(
#                 "execute() with no query called without prepare")
#
#         return super(PreparingCursor, self).execute(stmt, args)
#
#     def executemany(self, stmt, args=None):
#         if args is None:
#             args = stmt
#             stmt = self._execstmt
#
#             if stmt is None:
#                 raise psycopg2.ProgrammingError(
#                     "executemany() with no query called without prepare")
#         else:
#             if stmt != self._prepared:
#                 self.prepare(stmt)
#
#         return super(PreparingCursor, self).executemany(self._execstmt, args)
#
#     def close(self):
#         if not self.closed and not self.connection.closed and self._prepared:
#             self.deallocate()
# 
#         return super(PreparingCursor, self).close()


def main():
    f = subprocess.Popen(['tail','-F',"-n+1","/data/logs/kong/file.json"], stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    p = select.poll()
    p.register(f.stdout)

    while True:
        if p.poll(1):
            s=str(f.stdout.readline())
            s=s[2:-3]
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

    if i==0:
        hash_object = hmac.new(b'testkey',"smartcity".encode("UTF-8"),digestmod=hashlib.sha512)
        hex_dig = hash_object.hexdigest()
        temp.insert(3,hex_dig)
    else:
        prev_hash=str(rows[-1]).split("~")[1]
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

    i+=1

    if(i==100):

        # global threads
        #
        # for t in threads:
        #     if t.is_alive():
        #         t.join()
        #     else:
        #         threads.remove(t)
        i=0
        thr = threading.Thread(target=batch_insert, args=([rows]), kwargs={})
        thr.start()
        # threads.append(thr)
        rows=rows[-1:]

# def batch_insert(row_list):
#
#     conn = psycopg2.connect(database="postgres", user="postgres", password="password", host="127.0.0.1", port="5432")
#     cur = conn.cursor(cursor_factory=PreparingCursor)
#     cur.prepare("insert into logs(logline,hash)values(%s,%s)")
#     try:
#         # cur.executemany("insert into logs(logline,hash) values(%(first)s,%(second)s)",row_list)
#         # dataText = ','.join(cur.mogrify('(%s,%s)', row) for row in row_list)
#         # cur.execute('insert into logs(logline,hash)values ' + dataText)
#         for row in row_list:
#             log=row.split("~")
#             cur.execute((log[0],log[1]))
#         conn.commit()
#         conn.close()
#     except psycopg2.DatabaseError as e:
#         print(e)

def batch_insert(row_list):
    conn = psycopg2.connect(database="postgres", user="postgres", password="password", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    try:
        for row in row_list:
            log=row.split("~")
            cur.execute("insert into logs(logline,hash) values(%s,%s)",(log[0],log[1]))

        conn.commit()
        conn.close()
    except psycopg2.DatabaseError as e:
        print(e)


i = 0
rows=[]
#threads=[]
main()
