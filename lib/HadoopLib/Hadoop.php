<?php
/**
 * @author Ihor Burlachenko
 */

namespace HadoopLib;

class Hadoop {

    /**
     * Path to the Hadoop
     * 
     * @var string
     */
    private $hadoopPath;

    /**
     * @var \HadoopLib\Hadoop\Shell
     */
    private $shell;

    /**
     * @var \HadoopLib\Hadoop\FileSystem
     */
    private $fileSystem;

    /**
     * @param string $hadoopPath Path to the Hadoop
     */
    public function __construct($hadoopPath) {
        $this->hadoopPath = (string) $hadoopPath;
        $this->shell = new Hadoop\Shell($this->hadoopPath);
        $this->fileSystem = new \HadoopLib\Hadoop\FileSystem($this->shell);
    }

    /**
     * @param string $jobName
     * @param string $jobCacheDir
     * @return \HadoopLib\Hadoop\Job
     */
    public function createJob($jobName, $jobCacheDir) {
        return new \HadoopLib\Hadoop\Job($jobName, $this->shell, $this->fileSystem, $jobCacheDir);
    }
}