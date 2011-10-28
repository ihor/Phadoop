#!/usr/bin/env php
<?php

$word2count = array();

while (($line = fgets(STDIN)) !== false) {
   // remove leading and trailing whitespace and lowercase
   $line = strtolower(trim($line));
   // split the line into words while removing any empty string
   $words = preg_split('/\W/', $line, 0, PREG_SPLIT_NO_EMPTY);
   // increase counters
   foreach ($words as $word) {
       $word2count[$word] += 1;
   }
}

// write the results to STDOUT (standard output)
// what we output here will be the input for the
// Reduce step, i.e. the input for reducer.py
foreach ($word2count as $word => $count) {
   // tab-delimited
   echo $word, chr(9), $count, PHP_EOL;
}