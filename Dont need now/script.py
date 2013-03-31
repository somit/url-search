import MySQLdb as mdb
import json
import urllib2

from connection import URL_DATABASE
from connection import SOLR_CONFIG

def wholequery(url_id):

    con = mdb.connect(URL_DATABASE['host'], URL_DATABASE['user'],URL_DATABASE['password'],URL_DATABASE['database'])

    with con:
        try:
            cur = con.cursor(mdb.cursors.DictCursor)
            users_query = """select CONVERT(url_id, CHAR(8)) as id,url,description,UNIX_TIMESTAMP(created) as created,CONVERT(uid, CHAR(8)) as uid from url_desc where url_id in (""" + ",".join(url_id) + """)""" 
            cur.execute(users_query)
            rows = cur.fetchall()
            users = list(rows)
            raw_json = json.dumps(users, encoding="ISO-8859-1")
            update_in_solr(raw_json)
            return True
        except:
            return False

def update_in_solr(my_data):
    req = urllib2.Request(url=SOLR_CONFIG['url'],
                      data=my_data)
    req.add_header('Content-type', 'application/json')
    f = urllib2.urlopen(req)
    print f.read()
    
def main():
    
    url_id = find_updated_url_id()
    
    if url_id != False:
        run_wholequery(url_id)
    else:
        print "No url updated"

def find_updated_url_id():
    con = mdb.connect(URL_DATABASE['host'], URL_DATABASE['user'],URL_DATABASE['password'],URL_DATABASE['database'])
    with con:
        time_difference = int(SOLR_CONFIG["time_difference"])
        cur = con.cursor(mdb.cursors.DictCursor)
        url_query = """select CONVERT(url_id, CHAR(8)) as id from url_desc where TIMESTAMPDIFF(MINUTE,created,FROM_UNIXTIME(UNIX_TIMESTAMP(NOW()))) <=%d""" %(time_difference)
        cur.execute(url_query)
        rows = cur.fetchall()
        urls = list(rows)
        map_url = dict([(u['id'], u) for u in urls])
    
    if not map_url:
        return False
    return map_url.keys()

def run_wholequery(url_id):
    print "Starting - wholequery cron"
    output = wholequery(url_id)
    print output
    
if __name__ == '__main__':
    main()