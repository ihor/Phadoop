<?php

namespace WordHistogramWithCombiner;

require __DIR__ . '/../../vendor/autoload.php';

$classLoader = new \Symfony\Component\ClassLoader\UniversalClassLoader();
$classLoader->registerNamespace('HadoopLib', '../../lib');
$classLoader->registerNamespace('WordHistogramWithCombiner', '../');
$classLoader->register();

//define('HADOOP_LIB_DEBUG', true);

$hadoop = new \HadoopLib\Hadoop('/usr/local/Cellar/hadoop');

$job = $hadoop->createJob('WordHistogramWithCombiner', 'Temp')
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