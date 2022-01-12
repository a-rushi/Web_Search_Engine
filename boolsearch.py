import argparse
import json
import re
import os
import time
from porterstemmer import * 

retrieved_documents = []
p = PorterStemmer()

def intersect(list_a,list_b):
    l1 = len(list_a)
    l2 = len(list_b)
    i = 0
    j = 0
    intersection = []
    while(i < l1 and j < l2):
        if(list_a[i] == list_b[j]):
            intersection.append(list_a[i])
            i = i + 1
            j = j + 1
        elif(list_a[i] < list_b[j]):
            i = i + 1
        else:
            j = j + 1
    return intersection

def ungap_encode(gap_encoded):
    ungap_encoded = []
    prev = 0
    for item in gap_encoded:
        ungap_encoded.append(prev+item)
        prev = prev+item
    return ungap_encoded

def decode_c0(token):
    decoded_list = []
    token_info = dictionary[token]
    start_add = token_info[0]
    byte_length = token_info[2]
    binaryFile.seek(start_add,0)
    i = 0
    while(i < byte_length):
        decoded_list.append(int.from_bytes(binaryFile.read(4),byteorder = 'big'))
        i = i + 4
    return decoded_list
            

def decode_c1(encoded_list):
    decoded_list = []
    to_be_added = 0
    for code in encoded_list:
        to_be_added = to_be_added*128
        to_be_added = to_be_added + code
        if(code >= 128):
            to_be_added = to_be_added - 128
        else:
            decoded_list.append(to_be_added)
            to_be_added = 0
    return decoded_list

def convToBin(x):
    bin_rep = bin(x)[2:].zfill(8)
    return bin_rep

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

def decode_c2(token,encoded_list):
    decoded_list = []
    bin_rep = ""
    for code in encoded_list:
        bin_rep = bin_rep + convToBin(code)
    l = len(bin_rep)
    i = dictionary[token][1]
    while(i < l):
        if(bin_rep[i] == "0"):
            decoded_list.append(1)
            i = i + 1
            continue
        llx = 1
        while(bin_rep[i] == "1"):
            llx = llx + 1
            i = i + 1
        lx_s = "1"
        i = i + 1
        for j in range(llx-1):
            lx_s = lx_s + bin_rep[i]
            i = i + 1
        lx = convToDec(lx_s)
        x_s = "1"
        for j in range(lx-1):
            x_s = x_s + bin_rep[i]
            i = i + 1
        x = convToDec(x_s)
        decoded_list.append(x)
    return decoded_list

def decode_c3(token):
    decoded_docs = []
    if token in dictionary_gap_encoded:
        token_info = dictionary_gap_encoded[token]
        start_add = token_info[0]
        byte_length = token_info[1]
        with open("gapindexfile_uncompressed.idx","rb") as binaryGapUc:
            binaryGapUc.seek(start_add,0)
            i = 0
            while(i < byte_length):
                decoded_docs.append(int.from_bytes(binaryGapUc.read(4),byteorder = 'big'))
                i = i + 4
        binaryGapUc.close() 
    return decoded_docs

def makeFile_c3():
    command = "python -m snappy -d " + args.indexfile + " gapindexfile_uncompressed.idx"
    os.system(command)

def retrieve(token,compression_type):
    relevant_docs = []
    if(compression_type == 3):
        relevant_docs = decode_c3(token)
    elif(compression_type == 0):
        relevant_docs = decode_c0(token)
    else:
        if token in dictionary:
            encoded_docs = []
            token_info = dictionary[token]
            start_add = token_info[0]
            byte_length = token_info[2]
            binaryFile.seek(start_add,0)
            i = 0
            while(i < byte_length):
                encoded_docs.append(int.from_bytes(binaryFile.read(1),byteorder = 'big'))
                i = i + 1
            if(compression_type == 1):
                relevant_docs = decode_c1(encoded_docs)
            elif(compression_type == 2):
                relevant_docs = decode_c2(token,encoded_docs)
    relevant_docs = ungap_encode(relevant_docs)
    return relevant_docs
        
def boolSearch(query,compression_type):
    counter = 0
    retrieved_documents = []
    for token in query:
        if(counter == 0):
            retrieved_documents = retrieve(token,compression_type)
        else:
            retrieved_documents = intersect(retrieved_documents,retrieve(token,compression_type))
        counter = 1
    return retrieved_documents

def stemmed(token):
    if(token.isalpha()):
        token = p.stem(token,0,len(token)-1)
    return token

def driver(compression_type):
    if(compression_type == 3):
        makeFile_c3()
    query_number = 0
    start = time.time()
    while True:
        query = []
        line = queryFile.readline()
        if not line:
            break
        token_list = re.split(",| |:|\\.|'|`|\n|\"|;|\[|\]|\{|\}|\(|\)",line) 
        for token in token_list:
            if(token == ''):
                continue
            token = token.lower()
            if token not in stop_words:
                token = stemmed(token)
                if token not in stop_words:
                    query.append(token)
        retrieved_documents = boolSearch(query,compression_type)
        for doc_id in retrieved_documents:
            resultFile.write("Q"+str(query_number)+" ")
            resultFile.write(map_docidx_docname[str(doc_id)] + " ")
            resultFile.write("1.0")
            resultFile.write("\n")
        query_number = query_number+1
    end = time.time()
    if(compression_type == 3):
        os.remove("gapindexfile_uncompressed.idx")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Document Retrieval for queries")
    parser.add_argument('--queryfile',type = str)
    parser.add_argument('--resultfile',type = str)
    parser.add_argument('--indexfile',type = str)
    parser.add_argument('--dictfile',type = str)
    args = parser.parse_args()

    queryFile = open(args.queryfile,"r")
    resultFile = open(args.resultfile,"w")
    binaryFile = open(args.indexfile,"rb")
    with open(args.dictfile,"r") as indexFile:
        to_be_loaded = json.load(indexFile)

    dictionary = to_be_loaded[0]
    stop_words = to_be_loaded[1]
    map_docidx_docname = to_be_loaded[2]
    dictionary_gap_encoded = to_be_loaded[3]
    compression_type = to_be_loaded[4]

    driver(compression_type)
