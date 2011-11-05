#!/usr/bin/env php
<?php

require_once '%UniversalClassLoaderPath%';
$classLoader = new \Symfony\Component\ClassLoader\UniversalClassLoader();
$classLoader->registerNamespace('HadoopLib', '%HadoopLibPath%');
$classLoader->registerNamespace('%ProjectNamespaceName%', '%ProjectNamespacePath%');
$classLoader->register();

$worker = new %ProjectWorkerClassName%();
%WorkerReflection%
$worker->handle();