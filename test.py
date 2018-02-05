import psycopg2,time
import hmac,hashlib
def check():
    conn = psycopg2.connect(database="postgres", user="postgres", password="password", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    cur.execute("select logline,hash from logs")
    rows=cur.fetchall()

    row_count=0
    prev_hash=""

    for row in rows:

       if row_count==0:

           seed_hash=hmac.new(b'wO5DWccVT8+vpSDKCPPN_NU4#3sU_Y8Vs!b9ftY|8iymQXphl_yf*yV72@AWpV$&*=67LTltScUS!v9Df|QYEG6X78C2a0d|?&i9WS4kgn1x1b4XBpJWuwR?bhGBu1yn',"smartcity".encode("UTF-8"),digestmod=hashlib.sha512).hexdigest()
           temp=row[0].split(" ")
           temp.insert(3,seed_hash)
           hash=hmac.new(b'wO5DWccVT8+vpSDKCPPN_NU4#3sU_Y8Vs!b9ftY|8iymQXphl_yf*yV72@AWpV$&*=67LTltScUS!v9Df|QYEG6X78C2a0d|?&i9WS4kgn1x1b4XBpJWuwR?bhGBu1yn'," ".join(temp).encode("UTF-8"),digestmod=hashlib.sha512)
           prev_hash=hash.hexdigest()

           if prev_hash!=str(row[1]):
               print("Not ok")

       else:
           temp = row[0].split(" ")
           temp.insert(3, prev_hash)
           hash=hmac.new(b'wO5DWccVT8+vpSDKCPPN_NU4#3sU_Y8Vs!b9ftY|8iymQXphl_yf*yV72@AWpV$&*=67LTltScUS!v9Df|QYEG6X78C2a0d|?&i9WS4kgn1x1b4XBpJWuwR?bhGBu1yn'," ".join(temp).encode("UTF-8"),digestmod=hashlib.sha512)
           prev_hash = hash.hexdigest()

           if prev_hash!=str(row[1]):
               print("Not ok")

       row_count+=1

if __name__ == '__main__':
  check()