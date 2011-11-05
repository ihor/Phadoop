<?php

namespace WordHistogram;

require_once '../../lib/Symfony/Component/ClassLoader/UniversalClassLoader.php';
$classLoader = new \Symfony\Component\ClassLoader\UniversalClassLoader();
$classLoader->registerNamespace('HadoopLib', '../../lib');
$classLoader->registerNamespace('WordHistogram', '../');
$classLoader->register();

$hadoop = new \HadoopLib\Hadoop('/usr/local/Cellar/hadoop');

$hadoop->createJob('WordHistogram', 'Temp')
    ->setMapper(new Mapper(true))
    ->setReducer(new Reducer(true))
    ->clearData()
    ->addTask('Hello World')
    ->addTask('Hello Hadoop')
    ->addTask('Hello Hadoop is much better than Hello World')
    //->addTask('Tasks/MapReduceTutorial.txt')
    ->putResultsTo('Temp/Results.txt')
    ->run(true);