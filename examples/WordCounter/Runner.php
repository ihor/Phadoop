<?php

namespace WordCounter;

require __DIR__ . '/../../vendor/autoload.php';

$classLoader = new \Symfony\Component\ClassLoader\UniversalClassLoader();
$classLoader->registerNamespace('Phadoop', __DIR__ . '/../..');
$classLoader->registerNamespace('WordCounter', __DIR__ . '/..');
$classLoader->register();

//define('PHADOOP_MAPREDUCE_DEBUG', true);

$mr = new \Phadoop\MapReduce('/usr/local/Cellar/hadoop');

$job = $mr->createJob('WordCounter', 'Temp')
    ->setMapper(new Mapper())
    ->setReducer(new Reducer())
    ->clearData()
    ->addTask('Hello World')
    ->addTask('Hello Hadoop')
    ->putResultsTo('Temp/Results.txt')
    ->run();

echo $job->getLastResults();