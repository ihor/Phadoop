<?php
/**
 * @author Ihor Burlachenko
 */

namespace Phadoop\MapReduce;

/**
 * @see http://hadoop.apache.org/common/docs/r0.15.2/streaming.html
 */
class Job
{
    /**
     * @var string
     */
    private $name;

    /**
     * @var \Phadoop\MapReduce\Shell
     */
    private $shell;

    /**
     * @var \Phadoop\MapReduce\FileSystem
     */
    private $fileSystem;

    /**
     * @var \Phadoop\MapReduce\Job\Worker\Mapper
     */
    private $mapper;

    /**
     * @var \Phadoop\MapReduce\Job\Worker\Reducer
     */
    private $reducer;

    /**
     * @var \Phadoop\MapReduce\Job\Worker
     */
    private $combiner;

    /**
     * @var array
     */
    private $streamingOptions;

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
     * @var string
     */
    private $lastResults;

    /**
     * @var \Phadoop\MapReduce\Job\CodeGenerator
     */
    private $_codeGenerator;

    /**
     * @param string $name
     * @param \Phadoop\MapReduce\Shell $shell
     * @param \Phadoop\MapReduce\FileSystem $fileSystem
     * @param string $cacheDir
     * @return \Phadoop\MapReduce\Job
     * @throws \InvalidArgumentException
     */
    public function __construct($name, Shell $shell, FileSystem $fileSystem, $cacheDir)
    {
        if (empty($name)) {
            throw new \InvalidArgumentException("Job name can't be empty");
        }

        $this->name = (string) $name;
        $this->shell = $shell;
        $this->fileSystem = $fileSystem;
        $this->taskCounter = 0;
        $this->setCacheDir($cacheDir);
        $this->streamingOptions = array();
    }

    /**
     * @throws \InvalidArgumentException
     * @param string $cacheDir
     * @return \Phadoop\MapReduce\Job
     */
    private function setCacheDir($cacheDir)
    {
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
     * @param \Phadoop\MapReduce\Job\Worker\Mapper $mapper
     * @return \Phadoop\MapReduce\Job
     */
    public function setMapper(Job\Worker\Mapper $mapper)
    {
        $this->mapper = $mapper;
        return $this;
    }

    /**
     * @param \Phadoop\MapReduce\Job\Worker\Reducer $reducer
     * @return \Phadoop\MapReduce\Job
     */
    public function setReducer(Job\Worker\Reducer $reducer)
    {
        $this->reducer = $reducer;
        return $this;
    }

    /**
     * @param \Phadoop\MapReduce\Job\Worker $combiner
     * @return \Phadoop\MapReduce\Job
     */
    public function setCombiner(Job\Worker $combiner)
    {
        $this->combiner = $combiner;
        return $this;
    }

    /**
     * @return bool
     */
    private function hasCombiner()
    {
        return !is_null($this->combiner);
    }

    /**
     * @return \Phadoop\MapReduce\Job
     */
    public function clearData()
    {
        $this->fileSystem->remove($this->name, true);
        return $this;
    }

    /**
     * @param string $key
     * @param mixed $data If empty then key is data
     * @return \Phadoop\MapReduce\Job
     */
    public function addTask($key, $data = null)
    {
        if (is_null($data)) {
            $data = $key;
            $key = null;
        }

        $this->taskCounter++;
        $taskHdfsFilePath = "{$this->getHdfsTasksDir()}/{$this->taskCounter}.tsk";
        if (is_file($data)) {
            $this->fileSystem->moveFromLocal($this->prepareTaskFromFile($key, $data), $taskHdfsFilePath);
        }
        else {
            $this->fileSystem->writeToFile(Job\IO\Data\Output::create($key, $data), $taskHdfsFilePath);
        }

        return $this;
    }

    /**
     * @param string $key
     * @param string $localFilePath
     * @return string
     */
    private function prepareTaskFromFile($key, $localFilePath)
    {
        $tasksDir = "{$this->cacheDir}/Tasks";
        if (!is_dir($tasksDir)) {
            mkdir($tasksDir);
            chmod($tasksDir, 0766);
        }

        $taskLocalFilePath = "$tasksDir/{$this->taskCounter}.tsk";
        file_put_contents($taskLocalFilePath, Job\IO\Data\Output::create($key, file_get_contents($localFilePath)));

        return $taskLocalFilePath;
    }

    /**
     * @return string
     */
    private function getHdfsTasksDir()
    {
        return $this->name . '/tasks';
    }

    /**
     * @return string
     */
    private function getHdfsResultsDir()
    {
        return $this->name . '/results';
    }

    /**
     * @param string $localFilePath
     * @return \Phadoop\MapReduce\Job
     */
    public function putResultsTo($localFilePath)
    {
        $this->resultsFileLocalPath = (string) $localFilePath;
        return $this;
    }

    /**
     * @param string $option
     * @param string $value
     * @return \Phadoop\MapReduce\Job
     */
    public function setStreamingOption($option, $value)
    {
        $this->streamingOptions[(string) $option] = (string) $value;
        return $this;
    }

    /**
     * @return \Phadoop\MapReduce\Job
     */
    public function run()
    {
        $this->assertCacheDirIsSet();
        $this->assertMapperIsSet();
        $this->assertReducerIsSet();

        $this->getCodeGenerator()->generateScript($this->mapper, $this->cacheDir . '/Mapper.php');
        $this->getCodeGenerator()->generateScript($this->reducer, $this->cacheDir . '/Reducer.php');

        $jobParams = array($this->getHadoopStreamingJarPath(), '-D mapred.output.compress=false');
        foreach ($this->streamingOptions as $option => $value) {
            $jobParams[] = "-D $option=$value";
        }

        $jobParams = array_merge($jobParams, array(
            'input' => $this->name . '/tasks/*',
            'output' => $this->name . '/results',
            'mapper' => $this->cacheDir . '/Mapper.php',
            'reducer' => $this->cacheDir . '/Reducer.php',
        ));

        if ($this->hasCombiner()) {
            $this->getCodeGenerator()->generateScript($this->combiner, $this->cacheDir . '/Combiner.php');
            $jobParams['combiner'] = $this->cacheDir . '/Combiner.php';
        }

        /**
         * @todo Populate mapper, reducer, combiner & other code to all Hadoop nodes
         */

        $this->shell->exec('jar', $jobParams);
        $this->rememberResults();

        return $this;
    }

    /**
     * @return \Phadoop\MapReduce\Job\CodeGenerator
     */
    private function getCodeGenerator()
    {
        if (is_null($this->_codeGenerator)) {
            $this->_codeGenerator = new Job\CodeGenerator();
        }

        return $this->_codeGenerator;
    }

    /**
     * @throws \UnexpectedValueException
     * @return void
     */
    private function assertCacheDirIsSet()
    {
        if (is_null($this->cacheDir)) {
            throw new \UnexpectedValueException("Cache dir isn't set");
        }
    }

    /**
     * @throws \UnexpectedValueException
     * @return void
     */
    private function assertMapperIsSet()
    {
        if (is_null($this->mapper)) {
            throw new \UnexpectedValueException("Mapper isn't set");
        }
    }

    /**
     * @throws \UnexpectedValueException
     * @return void
     */
    private function assertReducerIsSet()
    {
        if (is_null($this->reducer)) {
            throw new \UnexpectedValueException("Reducer isn't set");
        }
    }

    /**
     * @return string
     */
    private function getHadoopStreamingJarPath()
    {
        $streamingDirPath = "{$this->shell->getHadoopPath()}/contrib/streaming";
        return $streamingDirPath . '/' . system("ls $streamingDirPath | grep \"hadoop-streaming.*\.jar\"");
    }

    /**
     * @todo Return all results
     * @return string
     */
    private function getResultsFileHdfsPath()
    {
        return "{$this->getHdfsResultsDir()}/part-00000";
    }

    /**
     * @return \Phadoop\MapReduce\Job
     */
    private function rememberResults()
    {
        $resultsFile = $this->resultsFileLocalPath;
        if (is_null($resultsFile)) {
            $resultsFile = $this->cacheDir . '/Results.txt';
        }

        system("rm $resultsFile");
        $this->fileSystem->copyToLocal($this->getResultsFileHdfsPath(), $resultsFile);
        $this->lastResults = file_get_contents($resultsFile);

        return $this;
    }

    /**
     * @return string
     */
    public function getLastResults()
    {
        return $this->lastResults;
    }

}
