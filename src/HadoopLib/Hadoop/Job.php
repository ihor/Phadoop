<?php

namespace HadoopLib\Hadoop;

class Job {

    /**
     * @var string
     */
    private $name;

    /**
     * @var \HadoopLib\Hadoop\Shell
     */
    private $shell;

    /**
     * @var \HadoopLib\Hadoop\FileSystem
     */
    private $fileSystem;

    /**
     * @var \HadoopLib\Hadoop\Job\Worker\Mapper
     */
    private $mapper;

    /**
     * @var \HadoopLib\Hadoop\Job\Worker\Reducer
     */
    private $reducer;

    /**
     * @var string
     */
    private $cacheDir;

    /**
     * @var int
     */
    private $taskCounter;

    /**
     * @var string
     */
    private $resultsFileLocalPath;

    /**
     * @var \HadoopLib\Hadoop\Job\CodeGenerator
     */
    private $_codeGenerator;

    /**
     * @var \HadoopLib\Hadoop\Job\Encoder
     */
    private $_encoder;

    /**
     * @param string $name
     * @param \HadoopLib\Hadoop\Shell $shell
     * @param \HadoopLib\Hadoop\FileSystem $fileSystem
     * @return \HadoopLib\Hadoop\Job
     */
    public function __construct($name, Shell $shell, FileSystem $fileSystem) {
        if (empty($name)) {
            throw new \InvalidArgumentException("Job name can't be empty");
        }

        $this->name = (string) $name;
        $this->shell = $shell;
        $this->fileSystem = $fileSystem;
        $this->taskCounter = 0;
    }

    /**
     * @throws \InvalidArgumentException
     * @param string $cacheDir
     * @return \HadoopLib\Hadoop\Job
     */
    public function setCacheDir($cacheDir) {
        if (!file_exists($cacheDir)) {
            mkdir($cacheDir, 0777);
        }
        elseif (!is_dir($cacheDir)) {
            throw new \InvalidArgumentException(sprintf('"%s" is not a directory', $cacheDir));
        }

        $this->cacheDir = realpath((string) $cacheDir);
        return $this;
    }

    /**
     * @param \HadoopLib\Hadoop\Job\Worker\Mapper $mapper
     * @return \HadoopLib\Hadoop\Job
     */
    public function setMapper(Job\Worker\Mapper $mapper) {
        $this->mapper = $mapper;
        $this->mapper->setEncoder($this->getEncoder());
        return $this;
    }

    /**
     * @param \HadoopLib\Hadoop\Job\Worker\Reducer $reducer
     * @return \HadoopLib\Hadoop\Job
     */
    public function setReducer(Job\Worker\Reducer $reducer) {
        $this->reducer = $reducer;
        $this->reducer->setEncoder($this->getEncoder());
        return $this;
    }

    /**
     * @return \HadoopLib\Hadoop\Job
     */
    public function clearJobData() {
        $this->fileSystem->remove($this->name, true);
        return $this;
    }

    /**
     * @todo Support adding tasks from file also
     * @param mixed $task
     * @return \HadoopLib\Hadoop\Job
     */
    public function addTask($task) {
        $this->taskCounter++;
        $taskHdfsFilePath = "{$this->getHdfsTasksDir()}/{$this->taskCounter}.tsk";
        if (is_file($task)) {
            $this->fileSystem->moveFromLocal($this->encodeTaskFromFile($task), $taskHdfsFilePath);
        }
        else {
            $this->fileSystem->writeToFile($this->getEncoder()->encode($task), $taskHdfsFilePath);
        }

        return $this;
    }

    /**
     * @param string $localFilePath
     * @return string
     */
    private function encodeTaskFromFile($localFilePath) {
        $content = file_get_contents($localFilePath);

        $encodedTasksDir = "{$this->cacheDir}/Tasks";
        if (!is_dir($encodedTasksDir)) {
            mkdir($encodedTasksDir);
            chmod($encodedTasksDir, 0766);
        }

        $encodedTaskLocalFilePath = "$encodedTasksDir/{$this->taskCounter}.tsk";
        file_put_contents($encodedTaskLocalFilePath, $this->getEncoder()->encode($content));
        return $encodedTaskLocalFilePath;
    }

    /**
     * @return \HadoopLib\Hadoop\Job\Encoder
     */
    private function getEncoder() {
        if (is_null($this->_encoder)) {
            $this->_encoder = new Job\Encoder();
        }

        return $this->_encoder;
    }

    /**
     * @return string
     */
    private function getHdfsTasksDir() {
        return $this->name . '/tasks';
    }

    /**
     * @return string
     */
    private function getHdfsResultsDir() {
        return $this->name . '/results';
    }

    /**
     * @param string $localFilePath
     * @return \HadoopLib\Hadoop\Job
     */
    public function setResultsFileLocalPath($localFilePath) {
        $this->resultsFileLocalPath = (string) $localFilePath;
        return $this;
    }

    /**
     * @param bool $displayResults
     * @return void
     */
    public function run($displayResults = false) {
        $this->assertCacheDirIsSet();
        $this->assertMapperIsSet();
        $this->assertReducerIsSet();

        $this->getCodeGenerator()->generateScript($this->mapper, $this->cacheDir . '/Mapper.php');
        $this->getCodeGenerator()->generateScript($this->reducer, $this->cacheDir . '/Reducer.php');

        /**
         * @todo Populate mapper, reducer & other code to all Hadoop nodes
         * @todo Maybe pack everything in phar
         */

        $this->shell->exec('jar', array(
            $this->getHadoopStreamingJarPath(),
            'mapper' => $this->cacheDir . '/Mapper.php',
            'reducer' => $this->cacheDir . '/Reducer.php',
            'input' => $this->name . '/tasks/*',
            'output' => $this->name . '/results',
            'file' => $this->cacheDir . '/Mapper.php',
            'file' => $this->cacheDir . '/Reducer.php',
            'jobconf' => 'mapred.output.compress=false'
        ));

        if ($displayResults) {
            $this->displayResults();
        }

        if (!is_null($this->resultsFileLocalPath)) {
            system("rm {$this->resultsFileLocalPath}");
            $this->fileSystem->copyToLocal($this->getResultsFileHdfsPath(), $this->resultsFileLocalPath);
        }
    }

    /**
     * @return \HadoopLib\Hadoop\Job\CodeGenerator
     */
    private function getCodeGenerator() {
        if (is_null($this->_codeGenerator)) {
            $this->_codeGenerator = new Job\CodeGenerator();
        }

        return $this->_codeGenerator;
    }

    /**
     * @throws \UnexpectedValueException
     * @return void
     */
    private function assertCacheDirIsSet() {
        if (is_null($this->cacheDir)) {
            throw new \UnexpectedValueException("Cache dir isn't set");
        }
    }

    /**
     * @throws \UnexpectedValueException
     * @return void
     */
    private function assertMapperIsSet() {
        if (is_null($this->mapper)) {
            throw new \UnexpectedValueException("Mapper isn't set");
        }
    }

    /**
     * @throws \UnexpectedValueException
     * @return void
     */
    private function assertReducerIsSet() {
        if (is_null($this->reducer)) {
            throw new \UnexpectedValueException("Reducer isn't set");
        }
    }

    /**
     * @return string
     */
    private function getHadoopStreamingJarPath() {
        $streamingDirPath = "{$this->shell->getHadoopPath()}/contrib/streaming";
        return $streamingDirPath . '/' . system("ls $streamingDirPath | grep \"hadoop-streaming.*\.jar\"");
    }

    /**
     * @todo Return all results (this method will not work for all situations)
     * @return string
     */
    private function displayResults() {
        echo "Results:\n";
        $this->fileSystem->displayFileContent($this->getResultsFileHdfsPath());
    }

    /**
     * @return string
     */
    private function getResultsFileHdfsPath() {
        return "{$this->getHdfsResultsDir()}/part-00000";
    }
}