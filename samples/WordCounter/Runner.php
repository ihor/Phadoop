<?php

namespace WordCounter;

require_once '../../src/Symfony/Component/ClassLoader/UniversalClassLoader.php';
$classLoader = new \Symfony\Component\ClassLoader\UniversalClassLoader();
$classLoader->registerNamespace('HadoopLib', '../../src');
$classLoader->registerNamespace('WordCounter', '../');
$classLoader->register();

$hadoop = new \HadoopLib\Hadoop('/usr/local/Cellar/hadoop');

$hadoop->createJob('WordCounter')
    ->setCacheDir('Temp')
    ->setMapper(new Mapper())
    ->setReducer(new Reducer())
    ->clearJobData()
    ->addTask('Hello World')
    ->addTask('Hello Hadoop')
    ->setResultsFileLocalPath('Temp/Results.txt')
    ->run(true);