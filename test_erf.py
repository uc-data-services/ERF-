import erf

def test_erf():
    local_resids_and_dates = [(3, '01-02-2009'), (4065, '2011-05-02'), (1539, ' 2010-03-01')] 
    erf_res_ids_and_dates = [(3, '01-02-2009'), (4065, '2011-05-15'),  (3029, '2009-05-07')]
    #expect: update 4065, new 3029, unpub  1539
    erf.resids_needing_updating_and_adding(local_resids_and_dates, erf_res_ids_and_dates)
    