<?php

namespace WordHistogramWithSorting;

require __DIR__ . '/../../vendor/autoload.php';

$classLoader = new \Symfony\Component\ClassLoader\UniversalClassLoader();
$classLoader->registerNamespace('Phadoop', __DIR__ . '/../..');
$classLoader->registerNamespace('WordHistogramWithSorting', __DIR__ . '/..');
$classLoader->register();

//define('PHADOOP_MAPREDUCE_DEBUG', true);

$mr = new \Phadoop\MapReduce('/usr/local/Cellar/hadoop');

$wordHistogramJob = $mr->createJob('WordHistogramWithSorting/HistogramStep', 'Temp/Histogram')
    ->setMapper(new HistogramMapper())
    ->setReducer(new HistogramReducer())
    ->setCombiner(new HistogramCombiner())
    ->clearData()
    ->addTask('Hello World')
    ->addTask('Hello Hadoop')
    ->addTask('Hello Hadoop is much better than Hello World')
    ->addTask('Hello combiner')
    ->addTask('This phrase demonstrates how combiner works: combiner, combiner, combiner, combiner, combiner...')
    ->addTask('Tasks/MapReduceTutorial.txt')
    ->putResultsTo('Temp/Results.txt')
    ->run();

$sortingJob = $mr->createJob('WordHistogramWithSorting/SortingStep', 'Temp/Sorting')
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

