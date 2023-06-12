import os
import pickle
import math
import random
from string import ascii_lowercase

def count_ngrams(ngram_counts, N, data):
    for i in range(len(data) - N):
        ngram = data[i:i+N]
        ngram_counts[ngram] = ngram_counts.get(ngram, 0) + 1

def make_ngram_stat_database(data_dir, N, dest_filename):	
    print ("Building ngram statistics database")
    ngrams_per_lang = {}
    ngrams_total = {}
    for lang in os.listdir(data_dir):
        if lang not in ngrams_per_lang:
            ngrams_per_lang[lang] = {}
        for fname in os.listdir(os.path.join(data_dir, lang)):
            #fbase, fext = os.path.splitext(fname)
            with open(os.path.join(data_dir, lang, fname), 'rb') as f:
                data = f.read().decode("utf-8")
            count_ngrams(ngrams_per_lang[lang], N, data)
            count_ngrams(ngrams_total, N, data)
    #totals per filetype
    total_fileext = dict( (lang, sum(count for ngram, count in ngrams_per_lang[lang].items())) for lang in ngrams_per_lang)
    #frequencies per filetype
    frequencies = {}
    for fileext in ngrams_per_lang:
        frequencies[fileext] = {}
        for ngram, count in ngrams_per_lang[fileext].items():
            frequencies[fileext][ngram] = count * 1.0 / total_fileext[fileext]
    #total
    total = sum(ngrams_total.values())
    #frequencies total
    frequencies_total = {}
    for ngram, count in ngrams_total.items():
        frequencies_total[ngram] = count * 1.0 / total
    print (sorted(frequencies_total.items(), key=lambda kv:kv[1], reverse=True))
    #print (frequencies_total["us "])
    #dump file
    with open(dest_filename, "wb") as fout:
        pickle.dump(frequencies, fout, -1)
        pickle.dump(frequencies_total, fout, -1)

def load_ngram_stat_database(filename):
    with open(filename, "rb") as fin:
        frequencies = pickle.load(fin)
        frequencies_total = pickle.load(fin)
    return (frequencies, frequencies_total)

def dot_product(a,b):
    return sum(x*y for (x,y) in zip(a,b))
    
def norm(a):
    return math.sqrt(dot_product(a,a))

def cosine_similarity(tfidf_a, tfidf_b):
    return (dot_product(tfidf_a, tfidf_b) / (norm(tfidf_a) * norm(tfidf_b)))


def match_specific_language(text, ngramstats, lang):
    N, frequencies, frequencies_total, tfidf = ngramstats
    ngram_counts = {}
    count_ngrams(ngram_counts, N, text)
    ntotal = len(text) - 1
    results = {}
    a = [(count * 1.0 / ntotal) for ngram, count in ngram_counts.items()]
    b = [tfidf[lang].get(ngram, -10) for ngram, count in ngram_counts.items()]
    #print (lang + " " + str(tfidf[lang]))
    try:
        return cosine_similarity(a,b)
    except ZeroDivisionError:
        return -1
        


def match_language(text, ngramstats):
    N, frequencies, frequencies_total, tfidf = ngramstats
    ngram_counts = {}
    count_ngrams(ngram_counts, N, text)
    ntotal = len(text) - 1
    results = {}
    for lang in tfidf.keys():
        a = [(count * 1.0 / ntotal) for ngram, count in ngram_counts.items()]
        b = [tfidf[lang].get(ngram, 0) for ngram, count in ngram_counts.items()]
        results[lang] = cosine_similarity(a,b)
    return results
    
def open_ngramstats(filename, N):
    frequencies, frequencies_total = load_ngram_stat_database(filename)
    tfidf = {}
    # Assuming languages from the web appart from french contain 45 possible different characters
    # (26 chars, +approx 5 for entropy of uppercase, +5 for punctuation, +5 for special characters 
    # (e.g. in javascript or css)
    NgramFreqOther = 1.0 / (41**N) 
    for lang, freqs in frequencies.items():
        tfidf[lang] = {}
        for gram, fq in frequencies[lang].items():
            tfidf[lang][gram] = math.log(fq / NgramFreqOther)
    return (N, frequencies, frequencies_total, tfidf)

def identify_language(text, ngramstats):
    results = match_language(text, ngramstats)
    return (sorted(results.items(), key=lambda kv:kv[0], reverse=True)[0][0])

if __name__ == '__main__':
    from optparse import OptionParser
    N=3
    parser = OptionParser()
    parser.add_option("-r", "--rewrite-database", dest="rewrite_database", action="store_true", help="Rewrites the n-gram database")
    parser.add_option("-d", "--db-name", dest="db_filename", type="string", help="Filename of the n-gram database", default="3gram-stats.dat")
    (options, args) = parser.parse_args()
    
    if options.rewrite_database or not os.path.exists(options.db_filename):
        make_ngram_stat_database("E:\\french\\data", N, options.db_filename)
    # load stat database
    ngramstats = open_ngramstats(options.db_filename, N)
    # identify language
    print (match_specific_language("Nous vous", ngramstats, "fr"))
    print (match_specific_language("auràua àçauz eràçauz rçàu", ngramstats, "fr"))
    print (match_specific_language("aioaziopazoipaoziopazopaiz", ngramstats, "fr"))
    print (match_specific_language("Les pieds dans le plat", ngramstats, "fr"))
    print (match_specific_language(" var ga = document.createElement('script'); ga.type = 'text/javascript'; ga.async = true;", ngramstats, "fr"))
    print (match_specific_language("Remplissez votre email pour obtenir votre mot de passe", ngramstats, "fr"))
    print (match_specific_language("aertaijtiojaijor jreoijo iajiooijaizojojigrje", ngramstats, "fr"))
    print ("--")

    print (match_specific_language("europeennes yannick jadot refuse l alliance proposee par segolene royal", ngramstats, "fr"))
    print (match_specific_language("temps de tout reinventer segolene royal a un flair politique extraordinaire mais l ecologie n est pas une mode Pour nous c est le combat d une vie", ngramstats, "fr"))
    print (match_specific_language("temps de tout reinventer segolene royal a un flair politique extraordinaire", ngramstats, "fr"))
    print (match_specific_language("tzadiajdij aoijdioaj iji ooija diojaiuizeuio iji ooija diojaiuizeuioerv", ngramstats, "fr"))
    print (match_specific_language("eleleleeeeeeeeeeeeeeeeeelleleleelleeeeeeeeeeeeeeellllllllllllllleeelell", ngramstats, "fr"))
    print (match_specific_language("".join(random.choice(ascii_lowercase + " ") for _ in range(50)).encode(), ngramstats, "fr"))
    print (match_specific_language("tempsdetoutreinventersegoleneroyalaunflairpolitiqueextraordinaire", ngramstats, "fr"))
    print (match_specific_language("malgrelabaissedelamobilisationdenewtonajourneedaction", ngramstats, "fr"))