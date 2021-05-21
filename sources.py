import glob
import json
from collections import Counter
import unicodedata


def read_unique_rew_batches(datadir): # this is for source distribution
    batchdict = {} # basename -> full path
    batchfiles=sorted(glob.glob(datadir+"/batches-*/*.json")+glob.glob(datadir+"/batches-*/*/*.json"))
    print(f"{len(batchfiles)} batches read.")
    for b in batchfiles:
        components=b.split("/")
        fname=components[-1]
        if fname in batchdict:
            continue # skip double annotated file
        batchdict[fname] = b
    print(f"{len(batchdict)} unique batches read.")    
    return batchdict
    
def read_unique_pick_batches(datadir): # yield, coverage, false positives
    batchdict = {} # basename -> full path
    batchfiles=sorted(glob.glob(datadir+"/batches-*/archived/*.json")) #glob.glob(datadir+"/batches-*/*.json")
    
    print(f"{len(batchfiles)} batches read.")
    for b in batchfiles:
        components=b.split("/")
        fname=components[-1]
        if fname in batchdict:
            continue # skip double annotated file
        if "_r2.json" in fname or "titles" in fname: # skip news titles and round 2 movies
            continue
        batchdict[fname] = b
    print(f"{len(batchdict)} unique batches read.")    
    return batchdict
    
def get_source(basename):
    if "exam" in basename:
        return "exam"
    elif "trans" in basename:
        return "trans"
    elif "s24" in basename:
        return "Suomi24"
    elif "titles" in basename or "news" in basename:
        return "news"
    else:
        return "movie"
        
def norm_text(txt):
    # html markup
    txt = txt.replace("<i>"," ").replace("</i>"," ").replace("</ i>", " ").replace("<b>"," ").replace("</b>"," ").replace("<font>"," ").replace("</font>"," ")
    # remove all but alphanumeric characters
    chars=[]
    for char in txt:
        if not char.strip(): # whitespace
            continue
        cat = unicodedata.category(char)
        if cat.startswith("L") or cat.startswith("N"): # take only letters and numbers
            chars.append(char)       
    return "".join(chars)
    
def coverage(txt, para):
    txt = norm_text(txt)
    para = norm_text(para)
    return len(para)/len(txt)*100, len(txt)
        

def segment_coverage(segment):
    coverages = []
    lengths = []
    annotation = segment.get("annotation", [])
        
    # original text
    text1 = segment["d1_text"]
    text2 = segment["d2_text"]
    
    para1, para2 = [], []

    for para in annotation:
        p1, p2 = para["txt"].split("\n")
        para1.append(p1)
        para2.append(p2)
    
    c, l = coverage(text1, "\n".join(para1))
    coverages.append(c)
    lengths.append(l)
    c, l = coverage(text2, "\n".join(para2))
    coverages.append(c)
    lengths.append(l)
    
    return coverages, lengths
    
def count_paraphrases(batchdict):
    counter = Counter()
    for basename, fullname in batchdict.items():
        with open(fullname, "rt", encoding="utf-8") as f:
            data = json.load(f)
        paraphrases = len(data) # TODO: rewrites
        source = get_source(basename)
        counter.update({source: paraphrases})
    return counter
    
    
def count_yield(batchdict):
    paraphrase_counter = Counter()
    file_counter = Counter()
    for basename, fullname in batchdict.items():
        with open(fullname, "rt", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("annotation_ready", False) == False:
            print("Annotation not marked ready:", fullname)
            continue       
        source = get_source(basename)
        for segment in data.get("segments", []):
            doc1 = segment.get("d1_text", None)
            doc2 = segment.get("d2_text", None)
            paraphrases = len(segment.get("annotation", []))
            if paraphrases == 0: # skip segment wihtout annotation
                continue
            paraphrase_counter.update({source: paraphrases})
            file_counter.update({source: 1})
    return paraphrase_counter, file_counter


def source_stats():

    batch_files = read_unique_rew_batches("/home/ginter/ann_data/news_titles_assigned")
    sources = count_paraphrases(batch_files)
    return sources
    
def paraphrases_per_segment():

    batch_files = read_unique_pick_batches("/home/ginter/pick_ann_data_live_new")
    paraphrases, segments = count_yield(batch_files)
    return paraphrases, segments
    
def paraphrase_coverage():

    all_coverage = {}
    false_positives = {}
    total_files = {}
    all_length = {}

    batch_files = read_unique_pick_batches("/home/ginter/pick_ann_data_live_new")
    for basename, fullname in batch_files.items():
        with open(fullname, "rt", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("annotation_ready", False) == False:
            print("Annotation not marked ready:", fullname)
            continue            
        source = get_source(basename)
        if source not in all_coverage:
            all_coverage[source] = []
            false_positives[source] = 0
            total_files[source] = 0
            all_length[source] = []
        annotation_found = False
        for segment in data.get("segments", []):
            if len(segment.get("annotation", [])) > 0:
                annotation_found = True
                cov, l = segment_coverage(segment)
                all_coverage[source] += cov
                all_length[source] += l
                
        if annotation_found == False:
            false_positives[source] += 1
        total_files[source] += 1
                
    for s in all_coverage.keys():
        print(f"\n{s}:")
        print(f"Percentage of empty files: {round(false_positives[s]/total_files[s]*100, 2)} ({false_positives[s]}/{total_files[s]})")
        print(f"Avegare coverage (aplhanumeric chars): {round(sum(all_coverage[s])/len(all_coverage[s]), 2)}% \n")
        print(f"Avegare length (aplhanumeric chars): {round(sum(all_length[s])/len(all_length[s]), 2)} \n")

    
    
    
if __name__ == '__main__':

    # source distribution
    sources = source_stats()
    for s, c in sources.most_common(100):
        print(f"{s}: {c} paraphrases ({round(c/sum(sources.values())*100, 2)} %)")
        
    # paraphrases per segment
    pairs, segments = paraphrases_per_segment()
    for s,c in segments.most_common(100):
        paraphrases = pairs[s]
        print(f"{s}: {paraphrases} paraphrases in {c} segments ({round(paraphrases/c, 2)} pairs/segment)")
        
    # paraphrase coverage and number of empty files (false positives)
    paraphrase_coverage()
    
