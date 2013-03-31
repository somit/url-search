from django.http import HttpResponse, HttpResponseBadRequest
import json
import MySQLdb as mdb
from django.shortcuts import render_to_response
from url_app.settings import URL_DATABASE
from url_app.settings import SOLR_CONFIG
import urllib2
from datetime import datetime


def index(request):
    return render_to_response('index.html')

def urls(request,uid):
    if request.method == 'POST':
        url = request.REQUEST.get('url','')
        desc = request.REQUEST.get('desc','')

        if desc == '' or url == '':
            error = {
                     "description":"desc or url should not null"
                     }
            error_response = {"error":error}
            error_response = json.dumps(error_response, encoding="ISO-8859-1")
            return HttpResponseBadRequest(error_response,content_type="application/json")
         
        try:
            response = urllib2.urlopen(url)
            response.read()
        except:
            error = {
                     "description":"url not correct - it should be please include http"
                     }
            error_response = {"error":error}
            error_response = json.dumps(error_response, encoding="ISO-8859-1")
            return HttpResponseBadRequest(error_response,content_type="application/json")
        
        con = mdb.connect(URL_DATABASE['host'], URL_DATABASE['user'],URL_DATABASE['password'],URL_DATABASE['database'])
        with con:
            url_query = """INSERT INTO url_desc(uid,url,description) VALUES ('%s','%s','%s')""" %(uid,url,desc)
            cur = con.cursor(mdb.cursors.DictCursor)
            cur.execute(url_query)
            urlid_query = """SELECT url_id FROM url_desc ORDER BY url_id DESC LIMIT 1"""
            cur.execute(urlid_query)
            rows = cur.fetchall()
            url_ids = list(rows)
            for one_id in url_ids:
                url_id = one_id["url_id"]
            
            url_folder = {
                    "data":
                            {
                              "url_id":url_id,  
                              "description": "updated in sql"
                             }
                    }
            output = update_in_solr(url_id)
            print output        
            url_folder = json.dumps(url_folder, encoding="ISO-8859-1")
            return HttpResponse(url_folder,content_type="application/json")
        
    elif request.method == 'GET':
        con = mdb.connect(URL_DATABASE['host'], URL_DATABASE['user'],URL_DATABASE['password'],URL_DATABASE['database'])
        with con:
                url_query = """select url_id as id,url ,description,date_format(created, '%%d-%%M-%%Y') as created from url_desc where uid = %s"""%(uid)
                cur = con.cursor(mdb.cursors.DictCursor)
                cur.execute(url_query)
                results = cur.fetchall()
                results = list(results)
                data = {"url_folder":results }
                results = json.dumps(data, encoding="ISO-8859-1")
                return HttpResponse(results,content_type="application/json")
    else:
        return HttpResponseBadRequest(content = "there is something wrong")
    
def update_in_solr(url_id):
    con = mdb.connect(URL_DATABASE['host'], URL_DATABASE['user'],URL_DATABASE['password'],URL_DATABASE['database'])
    with con:
        try:
            cur = con.cursor(mdb.cursors.DictCursor)
            url_query = """select CONVERT(url_id, CHAR(8)) as id,url,description,UNIX_TIMESTAMP(created) as created,CONVERT(uid, CHAR(8)) as uid from url_desc where url_id = %d"""%(int(url_id)) 
            cur.execute(url_query)
            rows = cur.fetchall()
            users = list(rows)
            raw_json = json.dumps(users, encoding="ISO-8859-1")
            req = urllib2.Request(url=SOLR_CONFIG['url'],
                      data=raw_json)
            req.add_header('Content-type', 'application/json')
            f = urllib2.urlopen(req)
            print f.read()
            return True
        except:
            return False
        
def search(request,uid):
    
    offset = request.REQUEST.get("offset",0)
    limit =  request.REQUEST.get("limit",10)
    query =  request.REQUEST.get("query","")
    
    if query == '':
        return HttpResponseBadRequest(content = "query should not be null")
    else:
        query = urllib2.quote(query)
        url = """http://localhost:8983/solr/collection1/select?q={!type=dismax%%20qf=description%%20v='%s'%%20bf=created}&fq=uid:%s&wt=json&start=%s&rows=%s"""%(query,uid,offset,limit)
        
        response = urllib2.urlopen(url)
        html = response.read()
        responsed = json.loads(html)
        
        urllist = list(responsed["response"]["docs"])
        newurllist = []
        for each_url in urllist:
            new_response_map = {}
            if "created" in each_url:
                new_response_map["created"] = datetime.fromtimestamp(each_url["created"]).strftime('%d-%b-%Y')
            if "url" in each_url:
                new_response_map["url"] = each_url["url"]
            if "description" in each_url:
                new_response_map["description"] = each_url["description"]
            if "id" in each_url:
                new_response_map["id"] = each_url["id"]
            if "uid" in each_url:
                new_response_map["uid"] = each_url["uid"]
            newurllist.append(new_response_map)
            
        metadata = {"url":url,"offset":int(offset),"total_result":responsed["response"]["numFound"],"limit":int(limit)}
        output = {"metadata":metadata,"urls":newurllist}
        urls = json.dumps(output, encoding="ISO-8859-1")
        
        return HttpResponse(urls,content_type="application/json")
        