import erf
import random
import sqlite3

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

def test_erf():
    #local_resids_and_dates = [(3, '01-02-2009'), (4065, '2011-05-02'), (1539, ' 2010-03-01')] 
    #erf_res_ids_and_dates = [(3, '01-02-2009'), (4065, '2011-05-15'),  (3029, '2009-05-07')]
    ##expect: update 4065, new 3029, unpub  1539
    erf.resources_needing_updating_and_adding(local_resids_and_dates, erf_res_ids_and_dates)
    erf_dict = erf.parse_page(html)
    #print erf_dict

def get_random_e_resource_ids(number=2):
    """
    Gets a random number of resource_ids from local db for use in testing updating or canceling erf resources.
    """
    sql_file = 'erf.sqlite'
    e_resource_query = "SELECT resource_id FROM resource"
    with sqlite3.Connection(sql_file) as conn:
        cursor = conn.cursor()
        cursor.execute(e_resource_query)
        resource_ids = random.sample(cursor.fetchall(), number)
        print(resource_ids)
    return resource_ids

