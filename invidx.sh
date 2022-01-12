#!/bin/bash

python3 invidx_cons.py --coll_path "${1}" --indexfile $2 --stopwordfile "${3}" --compression_type $4 --xml_tags_info "${5}"