#!/usr/bin/env php
<?php

$word2count = array();

// input comes from STDIN
while (($line = fgets(STDIN)) !== false) {
    // remove leading and trailing whitespace
    $line = trim($line);
    // parse the input we got from mapper.php
    list($word, $count) = explode(chr(9), $line);
    // convert count (currently a string) to int
    $count = intval($count);
    // sum counts
    if ($count > 0) $word2count[$word] += $count;
}

// sort the words lexigraphically
//
// this set is NOT required, we just do it so that our
// final output will look more like the official Hadoop
// word count examples
ksort($word2count);

// write the results to STDOUT (standard output)
foreach ($word2count as $word => $count) {
    echo $word, chr(9), $count, PHP_EOL;
}