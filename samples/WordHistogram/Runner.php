<?php

namespace WordHistogram;

require_once '../../src/Symfony/Component/ClassLoader/UniversalClassLoader.php';
$classLoader = new \Symfony\Component\ClassLoader\UniversalClassLoader();
$classLoader->registerNamespace('HadoopLib', '../../src');
$classLoader->registerNamespace('WordHistogram', '../');
$classLoader->register();

$hadoop = new \HadoopLib\Hadoop('/usr/local/Cellar/hadoop');

$hadoop->createJob('WordHistogram')
    ->setCacheDir('Temp')
    ->setMapper(new Mapper())
    ->setReducer(new Reducer())
    ->clearJobData()
    ->addTask('Hello World')
    ->addTask('Hello Hadoop')
    //->addTask('Tasks/MapReduceTutorial.txt')
    ->setResultsFileLocalPath('Temp/Results.txt')
    ->run(true);