import argparse
import os
from os import listdir
from os.path import isfile, join
from bs4 import BeautifulSoup as bs
import time
import re
from porterstemmer import * 
import json

postings = {}
dictionary = {} 
dictionary_gap_encoded = {}
map_docidx_docname = {}
to_be_dumped = []
p = PorterStemmer()

def read_stop_words():
    stop_words = {}
    while True:
        line = stop_words_file.readline()
        if not line:
            break
        word = line.split()
        stop_words[word[0]] = None
    return stop_words

def read_tags():
    tags_info = []
    line_number = 0
    while True:
        line = tags_info_file.readline()
        if not line:
            break
        if(line_number > 0):
            tag = line.split()
            tags_info.append(tag[0])
        line_number = line_number + 1
    return tags_info

def encode_c0(gap_encoded):
    for doc_id in gap_encoded:
        binaryFile.write(doc_id.to_bytes(4,byteorder = 'big'))

def encode_c1(gap_encoded):
    encoded_list = []
    for doc_id in gap_encoded:
        block_no = 0
        encoded = []
        while(doc_id > 0):
            temp_val = doc_id%128
            doc_id = doc_id//128
            if(block_no > 0):
                temp_val = temp_val + 128
            encoded.append(temp_val)
            block_no = block_no + 1
        encoded.reverse()
        for term in encoded:
            encoded_list.append(term)
    return encoded_list
        
def convToDec(x_s):
    x_s = x_s[::-1]
    mult = 1
    x = 0
    l = len(x_s)
    for i in range(l):
        if(x_s[i] == "1"):
            x = x + mult
        mult = mult*2
    return x

def encode_c2(key,gap_encoded):
    encoded_list = []
    bin_rep = ""
    for doc_id in gap_encoded:
        term3 = ""
        while(doc_id > 0):
            term3 = term3 + str(doc_id%2)
            doc_id = doc_id//2
        lx = len(term3)
        term3 = term3[:-1]
        term3 = term3[::-1]
        term2 = ""
        while(lx > 0):
            term2 = term2 + str(lx%2)
            lx = lx//2
        llx = len(term2)
        term2 = term2[:-1]
        term2 = term2[::-1]
        ullx = ""
        for i in range(llx-1):
            ullx = ullx + "1"
        ullx = ullx + "0"
        bin_rep = bin_rep + ullx
        bin_rep = bin_rep + term2
        bin_rep = bin_rep + term3
    l = len(bin_rep)
    bin_rep = bin_rep[::-1]
    zero_count = 0
    while((l % 8) != 0):
        bin_rep = bin_rep + "0"
        l = l + 1
        zero_count = zero_count + 1
    dictionary[key][1] = zero_count
    bin_rep = bin_rep[::-1]
    i = 0
    while(i < l):
        temp_bin = ""
        for j in range(8):
            temp_bin = temp_bin + bin_rep[i]
            i = i + 1
        encoded_list.append(convToDec(temp_bin))
    return encoded_list
    

def encode_c3():
    indexfile = args.indexfile + ".idx"
    command = "python -m snappy -c gapindexfile.idx " + indexfile 
    os.system(command)
      
def make_binary(key,gap_encoded,compression_type):
    if(compression_type == 0):
        encode_c0(gap_encoded)
    elif(compression_type != 3):
        if(compression_type == 1):
            encoded_list = encode_c1(gap_encoded)
        elif(compression_type == 2):
            encoded_list = encode_c2(key,gap_encoded)
        encoded_list = bytearray(encoded_list)
        binaryFile.write(encoded_list)  

def stemmed(token):
    if(token.isalpha()):
        token = p.stem(token,0,len(token)-1)
    return token

def gap_encode(doc_ids,compression_type):
    gap_encoded = []
    prev = 0
    for curr_idx in doc_ids:
        gap_encoded.append(curr_idx-prev)
        if(compression_type == 3):
            gapBinaryFile.write((curr_idx-prev).to_bytes(4,byteorder = 'big'))
        prev = curr_idx
    return gap_encoded

def make_postings(inp_dir,index_tags,stop_words,compression_type):
    start1 = time.time()
    file_names = [f for f in listdir(inp_dir) if isfile(join(inp_dir, f))]
    token_count = 0
    doc_idx = 0
    for file in file_names:
        file = join(inp_dir,file)
        with open(file,"r") as  f:
            content = f.read()
            documents = content.split("<DOC>")
            for document in documents:
                if(document == ''):
                    continue
                document = "<DOC>" + document
                bs_content = bs(document,"xml")
                doc_id = bs_content.find("DOCNO").string
                doc_idx = doc_idx+1
                map_docidx_docname[doc_idx] = doc_id
                for curr_tag in index_tags:
                    info_collection = bs_content.find_all(curr_tag)
                    for temp_info in info_collection:
                        if(temp_info == None):
                            continue
                        info = temp_info.string
                        if(info == None):
                            continue
                        token_list = re.split(",| |:|\\.|'|`|\n|\"|;|\[|\]|\{|\}|\(|\)",info)
                        for token in token_list:
                            if(token == ''):
                                continue
                            token = token.lower()
                            if(token not in stop_words):
                                token = stemmed(token)
                                if(token not in stop_words):
                                    if(token not in postings):
                                        postings[token] = [doc_idx]
                                        dictionary[token] = []
                                        dictionary_gap_encoded[token] = []
                                        token_count = token_count + 1
                                    else:
                                        if(postings[token][-1] != doc_idx):
                                            postings[token].append(doc_idx)  
    start = time.time()
    for key in postings:
        start_add = binaryFile.seek(0,2)
        start_add_gap = gapBinaryFile.seek(0,2)
        dictionary[key].append(start_add)
        dictionary[key].append(0)
        dictionary_gap_encoded[key].append(start_add_gap)
        gap_encoded = gap_encode(postings[key],compression_type)
        make_binary(key,gap_encoded,compression_type)
        end_add = binaryFile.seek(0,2)
        end_add_gap = gapBinaryFile.seek(0,2)
        dictionary[key].append(end_add-start_add)
        dictionary_gap_encoded[key].append(end_add_gap-start_add_gap)
    if(compression_type == 3):
        encode_c3()
    end = time.time()
    to_be_dumped.append(dictionary)
    to_be_dumped.append(stop_words)
    to_be_dumped.append(map_docidx_docname)
    to_be_dumped.append(dictionary_gap_encoded)
    to_be_dumped.append(compression_type)
    json.dump(to_be_dumped,indexFile)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Boolean Retrieval Inverted Index Constructor")
    parser.add_argument('--coll_path',type = str)
    parser.add_argument('--indexfile',type = str)
    parser.add_argument('--stopwordfile',type = str)
    parser.add_argument('--compression_type',type = int)
    parser.add_argument('--xml_tags_info',type = str)
    args = parser.parse_args()
    
    inp_dir = args.coll_path
    stop_words_file = open(args.stopwordfile,"r")
    stop_words = read_stop_words()
    compression_type = args.compression_type
    tags_info_file = open(args.xml_tags_info,"r")
    index_tags = read_tags()
    binaryFile = open(args.indexfile+str(".idx"),"wb")
    gapBinaryFile = open("gapindexfile.idx","wb")
    indexFile = open(args.indexfile+str(".dict"),"w")
    if(compression_type == 4 or compression_type == 5):
        print("not implemented")
    else:
        make_postings(inp_dir,index_tags,stop_words,compression_type)
    gapBinaryFile.close()
    os.remove("gapindexfile.idx")
