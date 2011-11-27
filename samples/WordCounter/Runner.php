<?php

namespace WordCounter;

require_once '../../lib/Symfony/Component/ClassLoader/UniversalClassLoader.php';
$classLoader = new \Symfony\Component\ClassLoader\UniversalClassLoader();
$classLoader->registerNamespace('HadoopLib', '../../lib');
$classLoader->registerNamespace('WordCounter', '../');
$classLoader->register();

//define('HADOOP_LIB_DEBUG', true);

$hadoop = new \HadoopLib\Hadoop('/usr/local/Cellar/hadoop');

$hadoop->createJob('WordCounter', 'Temp')
    ->setMapper(new Mapper())
    ->setReducer(new Reducer())
    ->clearData()
    ->addTask('Hello World')
    ->addTask('Hello Hadoop')
    ->putResultsTo('Temp/Results.txt')
    ->run(true);