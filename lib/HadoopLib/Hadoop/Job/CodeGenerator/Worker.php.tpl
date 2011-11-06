#!/usr/bin/env php
<?php

defined('HADOOP_LIB_DEBUG') || define('HADOOP_LIB_DEBUG', %HadoopLibDebug%);

require_once '%UniversalClassLoaderPath%';
$classLoader = new \Symfony\Component\ClassLoader\UniversalClassLoader();
$classLoader->registerNamespace('HadoopLib', '%HadoopLibPath%');
$classLoader->registerNamespace('%ProjectNamespaceName%', '%ProjectNamespacePath%');
$classLoader->register();

$worker = new %ProjectWorkerClassName%();
$worker->handle();