<?php

namespace WordHistogramWithCombiner;

require __DIR__ . '/../../vendor/autoload.php';

$classLoader = new \Symfony\Component\ClassLoader\UniversalClassLoader();
$classLoader->registerNamespace('Phadoop', __DIR__ . '/../..');
$classLoader->registerNamespace('WordHistogramWithCombiner', __DIR__ .  '/..');
$classLoader->register();

//define('PHADOOP_MAPREDUCE_DEBUG', true);

$mr = new \Phadoop\MapReduce('/usr/local/Cellar/hadoop');

$job = $mr->createJob('WordHistogramWithCombiner', 'Temp')
    ->setMapper(new Mapper())
    ->setReducer(new Reducer())
    ->setCombiner(new Combiner())
    ->clearData()
    ->addTask('Hello World')
    ->addTask('Hello Hadoop')
    ->addTask('Hello combiner')
    ->addTask('This phrase demonstrates how combiner works: combiner, combiner, combiner, combiner, combiner...')
    //->addTask('Tasks/MapReduceTutorial.txt')
    ->putResultsTo('Temp/Results.txt')
    ->run();

echo $job->getLastResults();