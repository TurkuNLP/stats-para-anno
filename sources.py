import glob
import json
from collections import Counter


def read_unique_rew_batches(datadir):
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
        
    
    
def count_paraphrases(batchdict):
    counter = Counter()
    for basename, fullname in batchdict.items():
        with open(fullname, "rt", encoding="utf-8") as f:
            data = json.load(f)
        paraphrases = len(data) # TODO: rewrites
        source = get_source(basename)
        counter.update({source: paraphrases})
       

    return counter


def source_stats():

    batch_files = read_unique_rew_batches("/home/ginter/ann_data/news_titles_assigned")
    sources = count_paraphrases(batch_files)
    return sources
    
    
if __name__ == '__main__':

    sources = source_stats()

    for s, c in sources.most_common(100):
        print(f"{s}: {c} paraphrases ({round(c/sum(sources.values())*100, 2)} %)")
