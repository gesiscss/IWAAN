#!/usr/bin/env python
#coding=utf-8
# Function: translate the results from BTM
# Input:
#    mat/pw_z.k20

import sys
from collections import Counter
import numpy as np

# return:    {wid:w, ...}
def read_voca(pt):
    voca = {}
    for l in open(pt):
        wid, w = l.strip().split('\t')[:2]
        voca[int(wid)] = w
    return voca

def read_pz(pt):
    return [float(p) for p in open(pt).readline().split()]

def get_revision(row, token_id):
    if token_id in row['token_id']:
        return str(row['rev_id'])
    

def display_topics(model_dir, K, voca_pt, tokens_processed, lng, the_page):

    voca = read_voca(voca_pt)    
    W = len(voca)
    print('K:%d, n(W):%d' % (K, W))

    pz_pt = model_dir + 'k%d.pz' % K
    pz = read_pz(pz_pt)
    
    zw_pt = model_dir + 'k%d.pw_z' %  K
    k = 0
    topics = {}
    topics_words = []
    for l in open(zw_pt):
        vs = [float(v) for v in l.split()]
        wvs = zip(range(len(vs)), vs)
        wvs = sorted(wvs, key=lambda d:d[1], reverse=True)
        #tmps = ' '.join(['%s' % voca[w] for w,v in wvs[:10]])
        tmps = ' '.join(['%s:%f' % (voca[w],v) for w,v in wvs[:10]])
        topics_words.append([str(w) for w,v in wvs[:40]])
        rev = []
        for w,v in wvs[:10]:
            token_revs = tokens_processed.apply(lambda x: get_revision(x, w), axis=1).dropna().values
            rev.extend(token_revs)
        top_rev = [r for r, r_count in Counter(rev).most_common(3)]
        topics[pz[k]] = (tmps, top_rev)
        k += 1
        
    count = 1
    for pz in sorted(topics.keys(), reverse=True):
        print(f'Topic {count}:')
        print('p(z): %f\nTop words:\n%s\n' % (pz, topics[pz][0]))
        
        for rev in topics[pz][1]:
            url = f"https://{lng}.wikipedia.org/w/index.php?&title={the_page['title'].replace(' ', '_')}&diff={rev}"
            print(url)
        print('\n')
        count += 1
        
    return topics_words, topics
