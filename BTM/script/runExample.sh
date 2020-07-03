#!/bin/bash
# run an toy example for BTM

K=30   # number of topics

alpha=3
beta=0.01
niter=150
save_step=50

output_dir=../output/
model_dir=${output_dir}model/
mkdir -p $output_dir/model 

# the input docs for training
doc_pt=../input/input.txt
voca_pt=../input/vocab.txt
# echo "=============== Index Docs ============="
# # docs after indexing
# dwid_pt=${output_dir}doc_wids.txt
# # vocabulary file
# voca_pt=${output_dir}voca.txt
# python indexDocs.py $doc_pt $dwid_pt $voca_pt

## learning parameters p(z) and p(w|z)
echo "=============== Topic Learning ============="
W=12760 # vocabulary size
make -C ../src/
#echo "../src/btm est $K $W $alpha $beta $niter $save_step $doc_pt $model_dir"
../src/btm est $K $W $alpha $beta $niter $save_step $doc_pt $model_dir

## infer p(z|d) for each doc
echo "================ Infer P(z|d)==============="
echo "../src/btm inf sum_b $K $doc_pt $model_dir"
../src/btm inf sum_b $K $doc_pt $model_dir

## output top words of each topic
# echo "================ Topic Display ============="
# python topicDisplay.py $model_dir $K $voca_pt
