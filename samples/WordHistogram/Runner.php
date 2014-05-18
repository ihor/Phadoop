<?php

namespace WordHistogram;

require __DIR__ . '/../../vendor/autoload.php';

$classLoader = new \Symfony\Component\ClassLoader\UniversalClassLoader();
$classLoader->registerNamespace('Phadoop', __DIR__ . '/../..');
$classLoader->registerNamespace('WordHistogram', __DIR__ . '/..');
$classLoader->register();

//define('PHADOOP_MAPREDUCE_DEBUG', true);

$mr = new \Phadoop\MapReduce('/usr/local/Cellar/hadoop');

$job = $mr->createJob('WordHistogram', 'Temp')
    ->setMapper(new Mapper())
    ->setReducer(new Reducer())
    ->clearData()
    ->addTask('Hello World')
    ->addTask('Hello Hadoop')
    //->addTask('Tasks/MapReduceTutorial.txt')
    ->putResultsTo('Temp/Results.txt')
    ->run();

echo $job->getLastResults();