<?php

namespace WordHistogramWithCombiner;

require_once '../../lib/Symfony/Component/ClassLoader/UniversalClassLoader.php';
$classLoader = new \Symfony\Component\ClassLoader\UniversalClassLoader();
$classLoader->registerNamespace('HadoopLib', '../../lib');
$classLoader->registerNamespace('WordHistogramWithCombiner', '../');
$classLoader->register();

//define('HADOOP_LIB_DEBUG', true);

$hadoop = new \HadoopLib\Hadoop('/usr/local/Cellar/hadoop');

$hadoop->createJob('WordHistogramWithCombiner', 'Temp')
    ->setMapper(new Mapper())
    ->setReducer(new Reducer())
    ->setCombiner(new Combiner())
    ->clearData()
    ->addTask('Hello World')
    ->addTask('Hello Hadoop')
    ->addTask('Hello combiner')
    ->addTask('This phrase demonstrates usage of combiner, combiner, combiner, combiner, combiner...')
    //->addTask('Tasks/MapReduceTutorial.txt')
    ->putResultsTo('Temp/Results.txt')
    ->run(true);