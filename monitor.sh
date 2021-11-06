#!/bin/bash
for f in scrapes/crawl*
do
  echo -n $f
  echo -ne "\t"
  grep 'usable repos' $f | tail -n1
done
