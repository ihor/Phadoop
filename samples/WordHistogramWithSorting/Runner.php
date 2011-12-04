<?php

namespace WordHistogramWithSorting;

require_once '../../lib/Symfony/Component/ClassLoader/UniversalClassLoader.php';
$classLoader = new \Symfony\Component\ClassLoader\UniversalClassLoader();
$classLoader->registerNamespace('HadoopLib', '../../lib');
$classLoader->registerNamespace('WordHistogramWithSorting', '../');
$classLoader->register();

//define('HADOOP_LIB_DEBUG', true);

$hadoop = new \HadoopLib\Hadoop('/usr/local/Cellar/hadoop');

$wordHistogramJob = $hadoop->createJob('WordHistogramWithSorting/HistogramStep', 'Temp/Histogram')
    ->setMapper(new HistogramMapper())
    ->setReducer(new HistogramReducer())
    ->setCombiner(new HistogramCombiner())
    ->clearData()
    ->addTask('Hello World')
    ->addTask('Hello Hadoop')
    ->addTask('Hello Hadoop is much better than Hello World')
    ->addTask('Hello combiner')
    ->addTask('This phrase demonstrates how combiner works: combiner, combiner, combiner, combiner, combiner...')
    //->addTask('Tasks/MapReduceTutorial.txt')
    ->putResultsTo('Temp/Results.txt')
    ->run();

$sortingJob = $hadoop->createJob('WordHistogramWithSorting/SortingStep', 'Temp/Sorting')
    ->setMapper(new SortingMapper())
    ->setReducer(new SortingReducer())
    ->clearData()
    ->setStreamingOption('mapred.output.key.comparator.class', 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator')
    ->setStreamingOption('mapred.text.key.comparator.options', '-k1nr')
    ->putResultsTo('Temp/SortedResults.txt');

$sortingTasks = explode("\n", trim($wordHistogramJob->getLastResults()));
foreach ($sortingTasks as $task) {
    list ($word, $counter) = explode("\t", $task);
    $sortingJob->addTask($word, $counter);
}
$sortingJob->run();

echo $sortingJob->getLastResults();

