<?php
/**
 * @author Ihor Burlachenko
 */

namespace Phadoop;

class MapReduce
{
    /**
     * Path to the Hadoop
     * 
     * @var string
     */
    private $hadoopPath;

    /**
     * @var \Phadoop\MapReduce\Shell
     */
    private $shell;

    /**
     * @var \Phadoop\MapReduce\FileSystem
     */
    private $fileSystem;

    /**
     * @param string $hadoopPath Path to the Hadoop
     */
    public function __construct($hadoopPath)
    {
        $this->hadoopPath = (string) $hadoopPath;
        $this->shell = new MapReduce\Shell($this->hadoopPath);
        $this->fileSystem = new MapReduce\FileSystem($this->shell);
    }

    /**
     * @param string $jobName
     * @param string $jobCacheDir
     * @return \Phadoop\MapReduce\Job
     */
    public function createJob($jobName, $jobCacheDir)
    {
        return new MapReduce\Job($jobName, $this->shell, $this->fileSystem, $jobCacheDir);
    }

}
