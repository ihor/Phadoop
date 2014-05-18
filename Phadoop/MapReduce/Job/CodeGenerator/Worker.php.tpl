#!/usr/bin/env php
<?php

defined('PHADOOP_MAPREDUCE_DEBUG') || define('PHADOOP_MAPREDUCE_DEBUG', %PhadoopMapReduceDebug%);

require_once '%UniversalClassLoaderPath%';
$classLoader = new \Symfony\Component\ClassLoader\UniversalClassLoader();
$classLoader->registerNamespace('Phadoop', '%PhadoopPath%');
$classLoader->registerNamespace('%ProjectNamespaceName%', '%ProjectNamespacePath%');
$classLoader->register();

$worker = new %ProjectWorkerClassName%();
$worker->handle();