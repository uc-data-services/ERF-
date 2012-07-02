import erf
import random
import sqlite3
from nose.tools import *
import logging

TEST_DB = '/home/tim/Dropbox/erf-db-test/erf.sqlite'

html = '''<html><head>
<title>Electronic Resources-The Library-University of California, Berkeley</title><meta http-equiv="Content-type" content="text/html; charset=iso-8859-1" /><meta name="keywords" content="" /><meta name="description" content="" /><link rel="stylesheet" type="text/css" href="http://cluster4.lib.berkeley.edu:8080/ERF/images/library_pages_orig.css" />
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8"><LINK REL=StyleSheet HREF="http://cluster4.lib.berkeley.edu:8080/ERF/css/style.css" TYPE="text/css" MEDIA=screen> 
<script language="JavaScript">
</script>
<script type="text/javascript">  var _gaq = _gaq || [];  _gaq.push(['_setAccount', 'UA-22225155-1']);  _gaq.push(['_trackPageview']);  (function() {    var ga = document.createElement('script'); ga.type = 'text/javascript'; ga.async = true;    ga.src = ('https:' == document.location.protocol ? 'https://ssl' : 'http://www') +'.google-analytics.com/ga.js';    var s = document.getElementsByTagName('script')[0]; s.parentNode.insertBefore(ga, s);  })();</script><script type="text/javascript">function recordOutboundLink(link, category, action) {  try {    var pageTracker=_gat._getTracker("UA-22225155-1");    pageTracker._trackEvent(category, action);    setTimeout('document.location = "' + link.href + '"', 100)  }catch(err){}}</script>
</head>
<p><B>Title:</B> Humanities International Complete<BR> 
<B>Alternate title:</B> International Humanities Index<BR> 
<B>Alternate title:</B> American Humanities Index<BR> 
<B>URL:</B> <A HREF="http://search.ebscohost.com/login.aspx?authtype=ip,uid&profile=ehost&defaultdb=hlh">http://search.ebscohost.com/login.aspx?authtype=ip,uid&profile=ehost&defaultdb=hlh</A><BR> 
<B>Resource Type:</B> Article Databases<BR> 
<B>Subject:</B> Literature<BR> 
<B>Subject:</B> General<BR> 
<B>Subject:</B> Folklore<BR> 
<B>Subject:</B> Theater, Dance and Performance Studies<BR> 
<B>Subject:</B> English and American Literature<BR> 
<B>Subject:</B> Near Eastern Studies - Islamica<BR> 
<B>Subject:</B> Comparative Literature<BR> 
<B>Subject:</B> Middle Eastern Studies<BR> 
<B>Access:</B> UCB<BR> 
<B>Text:</B> Some full text<BR> 
<B>Brief description:</B> Indexes thousands of journals, books and other published sources from around the world, with full text of over 770 journals. Includes all data from Humanities International Index (over 2,000 titles and 2 million records).  Subjects covered include archaeology, literature, religion, art, dance, theater, folklore, history, African-American studies, law, women's studies, and more.<BR> 
<B>Publication dates covered:</B> 1927 - present<BR> 
<B>Access Service or Vendor:</B> EBSCOhost<BR> 
<B>UC-eLinks:</B> yes<BR>
<B>Selector contact:</B> M. Burnette, M. Cochran<BR> 
<B>Record added:</B> 2007-07-27<BR> 
<B>Record last modified:</B> 2008-03-05<BR> 
<B>Comment(s) about this record:</B> <A HREF="mailto:erfmgr@library.berkeley.edu">ERF Manager</A></p>
'''

def setup_func():
    """set up test fixtures"""
    print "setup"

def teardown_func():
    """tear down test fixtures"""
    print "tear down"

@with_setup(setup_func, teardown_func)
def test_resource_in_db():
    """
    testing function resource_in_db
    """
    id_in_db = get_random_e_resource_ids(test_db_connection(), 4)
    for id in id_in_db:
        print ("IN local sqlite_db", id, erf.resource_in_db(id, test_db_connection()))
    ids_not_in_db = random.sample(range(5000,10000), 10)
    for id in ids_not_in_db:
        print ("Not in db", id, erf.resource_in_db(id,test_db_connection()))

def test_get_page():
    """
    testing get_page function for three types of pages in our scrape
    """
    urls = ['http://cluster4.lib.berkeley.edu:8080/ERF/servlet/ERFmain?cmd=allResTypes', 'http://cluster4.lib.berkeley.edu:8080/ERF/servlet/ERFmain?cmd=searchResType&resTypeId=2', 'http://cluster4.lib.berkeley.edu:8080/ERF/servlet/ERFmain?cmd=detail&resId=4108']
    for url in urls:
        html = erf.get_page(url)


def test_parse_page():
    """
    testing parse_page function, sending some resource IDs to fetch
    """
    pass

def test_resource_needs_updating():
    """
    testing to see if a resource id & its update date for the erf  needs updating
    """
    pass

#TODO:identify the modules that call one another

def get_random_e_resource_ids(c, number=2):
    """
    Gets a random number of resource_ids from local db f#or use in testing updating or canceling erf resources.
    """
    e_resource_query = "SELECT resource_id FROM resource ORDER BY Random() LIMIT ?"
    c.execute(e_resource_query, (number,))
    #resource_ids = random.sample(c.fetchall(), number)
    resource_ids = c.fetchall()
    resource_ids =  [rid[0] for rid in resource_ids]
    print resource_ids
    return resource_ids

def test_db_connection():
    with sqlite3.connect(TEST_DB) as conn:
        c = conn.cursor()
    return c
