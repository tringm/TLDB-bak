#!/bin/bash
mkdir -p test/io/in/cases && rm -r test/io/in/cases/*
cd test/io/in/cases
FILENAME="cases.tar.gz"
FILEID="11Zamj_r2cnXVTXivRclgCV-_0MZqENxU"

wget --no-check-certificate "https://drive.google.com/uc?export=download&id=${FILEID}" -O $FILENAME

tar -xzf cases.tar.gz --strip 1 && rm cases.tar.gz
