from werkzeug.middleware.dispatcher import DispatcherMiddleware
import flask
from flask import Flask
from flask import render_template, request
import os
import glob
from sqlitedict import SqliteDict
import json
import datetime
import difflib
import html
import re
import sys
import glob
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import sklearn.metrics

import base64
import io

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True
APP_ROOT = os.environ.get('PARA_STATS_APP_ROOT',"")
app.config["APPLICATION_ROOT"] = APP_ROOT

sanitize_re=re.compile(r"[^a-zäöåA-ZÄÖÅ0-9 ]")
whitespace_re=re.compile(r"\s+")
def sanitize(txt):
    txt_clean=sanitize_re.sub("",txt) #remove weird characters and punctuation
    txt_clean=whitespace_re.sub(" ",txt_clean) #replace all whitespace with a single space
    return txt_clean.strip().lower() #strip and lowercase


def read_rew_batches(datadir):
    batchdict={} #user -> batchfile -> Batch
    batchfiles=sorted(glob.glob(datadir+"/batches-*/*.json")+glob.glob(datadir+"/batches-*/*/*.json"))
    for b in batchfiles:
        components=b.split("/")
        fname=components[-1]
        if "batches-" in components[-2]:
            user=components[-2].replace("batches-","")
        else:
            assert "batches-" in components[-3]
            user=components[-3].replace("batches-","")
        with open(b) as f:
            batchdict.setdefault(user,{})[fname]=json.load(f)
    return batchdict

def timeline(rew_batches):
    for user,batches in rew_batches.items():
        for fname,b in batches.items():
            for elem in b:
                lab=elem.get("annotation",{}).get("label",None)
                if lab is not None and "|" not in lab:
                    yield {"user":user,"timestamp":datetime.datetime.fromisoformat(elem["annotation"]["updated"]),"label":lab, "rew":(elem["annotation"]["rew1"].strip()!="")}

def unique_examples(batchdict):
    all_examples=[]

    for user,batches in batchdict.items():
        for fname,b in batches.items():
            all_examples.extend(b)

    unique={}
    unique_34={}
    unique_rew={}
    for e in all_examples:
        lab=e.get("annotation",{}).get("label")
        if lab is not None and "|" not in lab:
            #this is a real example
            texts=(sanitize(e["txt1"]),sanitize(e["txt2"]))
            unique.setdefault(texts,[]).append(e)
            if "3" in lab or "4" in lab:
                unique_34.setdefault(texts,[]).append(e)
            if e["annotation"]["rew1"].strip():
                rew_texts=(sanitize(e["annotation"]["rew1"]),sanitize(e["annotation"]["rew2"]))
                unique_rew.setdefault(rew_texts,[]).append(e)
    total_unique=sum(len(lst) for lst in unique.values())
    total_unique_rew=sum(len(lst) for lst in unique_rew.values())
    return len(unique),len(unique_34),len(unique_rew)
    #for k,v in unique.items():
    #    print(k)

def day(timestamp):
    return (timestamp.year,timestamp.month,timestamp.day)

def week(timestamp):
    return timestamp.isocalendar()[1]
    
def normlabel(l):
    l=l.strip()
    if not l:
        l="x"
    l=l.replace("j","i")
    numbers=[c for c in l if c.isnumeric()]
    rest=[c for c in l if not c.isnumeric()]
    if len(numbers)>1:
        numbers=sorted(numbers)[:1]
    if numbers and numbers[0]=="3":
        rest=[]
    return "".join(sorted(numbers))+"".join(sorted(rest))


def coarse_label(l):
    l=l.replace("<","A").replace(">","A")
    if (len(l)>2 and l[1]=="A"):
        l=l[:2]+"*"
    elif (len(l)>1 and l[1]!="A"):
        l=l[:1]+"*"
            
    return l

def agreement_timeline(batchdict,merged):
    gold={}
    for fname,b in merged.items():
        for elem in b:
            lab=elem.get("annotation",{}).get("label",None)
            if lab is not None and "|" not in lab:
                normlab=normlabel(lab)
                coarselab=coarse_label(normlab)
                gold[(sanitize(elem["txt1"]),sanitize(elem["txt2"]))]=(normlab,coarselab)
                
    for user,batches in batchdict.items():
        for fname,b in batches.items():
            for elem in b:
                lab=elem.get("annotation",{}).get("label",None)
                if lab is not None and "|" not in lab:
                    normlab=normlabel(lab)
                    coarselab=coarse_label(normlab)
                    texts=(sanitize(elem["txt1"]),sanitize(elem["txt2"]))
                    if texts in gold:
                        g_normlab,g_coarselab=gold[texts]
                        yield {"user":user,"timestamp":datetime.datetime.fromisoformat(elem["annotation"]["updated"]),"normlab":normlab,"coarselab":coarselab,"g_normlab":g_normlab,"g_coarselab":g_coarselab,"normlab_ok":normlab==g_normlab,"coarselab_ok":coarselab==g_coarselab}

@app.route('/')
def idxpage():
    batchdict=read_rew_batches("/home/ginter/rew-data")
    del batchdict["JennaK"]
    merged=batchdict["Merged"]
    del batchdict["Merged"]
    
    df=pd.DataFrame(timeline(batchdict))
    df["normlabel"]=df["label"].apply(normlabel)
    df["coarselabel"]=df["normlabel"].apply(coarse_label)
    df["day"]=df["timestamp"].apply(day)
    df["week"]=df["timestamp"].apply(week)

    #Progress per week
    week_done=df.groupby("week")["normlabel"].count().to_frame()
    plt.bar(x=week_done.index,height=list(week_done["normlabel"]))
    plt.title("Total done per week")
    dat=io.BytesIO()
    plt.savefig(dat)
    plt.close()
    weektot_str = "data:image/png;base64,"
    weektot_str += base64.b64encode(dat.getvalue()).decode('utf8')

    #Totals
    basic_counts=df.groupby("normlabel").count().sort_values(["label"],ascending=False)["label"].to_frame()
    rew_grand_total=int(basic_counts.sum())
    rew_rew_grand_total=int(df["rew"].sum())
    basic_counts_html=basic_counts.to_html(classes=["table","table-sm","table-hover"],header=False)
    coarse_counts=df.groupby("coarselabel").count().sort_values(["label"],ascending=False)["label"].to_frame().to_html(classes=["table","table-sm","table-hover"],header=False)

    unique_cls,unique_cls34,unique_rew=unique_examples(batchdict)

    elapsed=(datetime.datetime.now()-datetime.datetime(2020,9,17)).total_seconds()
    whole=(datetime.datetime(2021,4,30)-datetime.datetime(2020,9,17)).total_seconds()
    coeff=whole/elapsed*0.93


    #AGREEMENT DATA
    agg_df=pd.DataFrame(agreement_timeline(batchdict,merged))
    agg_df=agg_df[agg_df["timestamp"]>datetime.datetime(2020,9,28)]
    agg_df["week"]=agg_df["timestamp"].apply(week)
    #Delete spurious weeks with <20 hits these are probably hits in new data which accidentally match something in merged
    week_counts=agg_df.groupby("week")["user"].count()
    ok_weeks=week_counts[week_counts>20].index
    agg_df=agg_df[agg_df["week"].isin(ok_weeks)]

    agg_stats=agg_df.groupby(["user","week"]).mean()
    agg_stats_html=(agg_stats*100).to_html(classes=["table","table-sm","table-hover"],float_format=lambda x:"{x:2.03}".format(x=x))

    labels=["4","4A","4*","4A*","3","2"]
    matrices=[]
    for user_w,rows in agg_df.groupby(["user","week"]).groups.items():
        user,w=user_w
        dataf=agg_df.loc[list(rows),:]
        ytrue=list(dataf["g_coarselab"])
        yuser=(dataf["coarselab"])
        cfm=sklearn.metrics.confusion_matrix(ytrue,yuser,labels)
        cfm_d=sklearn.metrics.ConfusionMatrixDisplay(cfm,display_labels=labels)
        cfm_d.plot()
        plt.title(f"{user} at week {w}")
        dat=io.BytesIO()
        plt.savefig(dat)
        plt.close()
        matrices.append("data:image/png;base64,"+base64.b64encode(dat.getvalue()).decode('utf8'))
    
    return render_template("index.html",weektot=weektot_str,bcounts=basic_counts_html,ccounts=coarse_counts,rewtot=rew_grand_total,rewtot_rew=rew_rew_grand_total,unique_cls=unique_cls,unique_cls34=unique_cls34,unique_cls_rew=unique_rew,coeff=coeff,a_stats=agg_stats_html,a_matrices=matrices)
    
    

    #How many days did each annotator do?
    days=df.groupby("user")["day"].nunique()
    hours=days*7.25/2.0
    hours["Maija"]*=2.0
    hours["JennaS"]*=2.0

    #Annotated examples
    ann_ex=df.groupby("user")["normlabel"].count()
    ann_ex_per_hour=hours.to_frame().join(ann_ex.to_frame())
    ann_ex_per_hour["ex_per_h"]=ann_ex_per_hour.normlabel/ann_ex_per_hour.day
    print(ann_ex_per_hour["ex_per_h"])
    
    ann_ex=df[df.rew].groupby("user")["normlabel"].count()
    ann_ex_per_hour=hours.to_frame().join(ann_ex.to_frame())
    ann_ex_per_hour["ex_per_h"]=ann_ex_per_hour.normlabel/ann_ex_per_hour.day
    print(ann_ex_per_hour["ex_per_h"])



    x=list(y.index)
    plt.bar(x=x,height=y.to_numpy())
    plt.show()
    
    #y=df.groupby("user").count()["normlabel"]
    #plt.bar(x=list(y.index),height=y.to_numpy())
    #plt.show()

#idxpage()
