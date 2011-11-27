<?php
/**
 * @author Ihor Burlachenko
 */

namespace HadoopLib\Hadoop;

/**
 * @see http://hadoop.apache.org/common/docs/r0.15.2/streaming.html
 */
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
     * @var \HadoopLib\Hadoop\Job\Worker
     */
    private $combiner;

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
     * @var \HadoopLib\Hadoop\Job\CodeGenerator
     */
    private $_codeGenerator;

    /**
     * @param string $name
     * @param \HadoopLib\Hadoop\Shell $shell
     * @param \HadoopLib\Hadoop\FileSystem $fileSystem
     * @param string $cacheDir
     * @return \HadoopLib\Hadoop\Job
     */
    public function __construct($name, Shell $shell, FileSystem $fileSystem, $cacheDir) {
        if (empty($name)) {
            throw new \InvalidArgumentException("Job name can't be empty");
        }

        $this->name = (string) $name;
        $this->shell = $shell;
        $this->fileSystem = $fileSystem;
        $this->taskCounter = 0;
        $this->setCacheDir($cacheDir);
    }

    /**
     * @throws \InvalidArgumentException
     * @param string $cacheDir
     * @return \HadoopLib\Hadoop\Job
     */
    private function setCacheDir($cacheDir) {
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
        return $this;
    }

    /**
     * @param \HadoopLib\Hadoop\Job\Worker\Reducer $reducer
     * @return \HadoopLib\Hadoop\Job
     */
    public function setReducer(Job\Worker\Reducer $reducer) {
        $this->reducer = $reducer;
        return $this;
    }

    /**
     * @param \HadoopLib\Hadoop\Job\Worker $combiner
     * @return \HadoopLib\Hadoop\Job
     */
    public function setCombiner(Job\Worker $combiner) {
        $this->combiner = $combiner;
        return $this;
    }

    /**
     * @return bool
     */
    private function hasCombiner() {
        return !is_null($this->combiner);
    }

    /**
     * @return \HadoopLib\Hadoop\Job
     */
    public function clearData() {
        $this->fileSystem->remove($this->name, true);
        return $this;
    }

    /**
     * @param string $key
     * @param mixed $data If empty then key is data
     * @return \HadoopLib\Hadoop\Job
     */
    public function addTask($key, $data = null) {
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
            $this->fileSystem->writeToFile(new Job\IO\Data\Output($key, $data), $taskHdfsFilePath);
        }

        return $this;
    }

    /**
     * @param string $key
     * @param string $localFilePath
     * @return string
     */
    private function prepareTaskFromFile($key, $localFilePath) {
        $tasksDir = "{$this->cacheDir}/Tasks";
        if (!is_dir($tasksDir)) {
            mkdir($tasksDir);
            chmod($tasksDir, 0766);
        }

        $taskLocalFilePath = "$tasksDir/{$this->taskCounter}.tsk";
        file_put_contents($taskLocalFilePath, new Job\IO\Data\Output($key, file_get_contents($localFilePath)));

        return $taskLocalFilePath;
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
    public function putResultsTo($localFilePath) {
        $this->resultsFileLocalPath = (string) $localFilePath;
        return $this;
    }

    /**
     * @return \HadoopLib\Hadoop\Job
     */
    public function run() {
        $this->assertCacheDirIsSet();
        $this->assertMapperIsSet();
        $this->assertReducerIsSet();

        $this->getCodeGenerator()->generateScript($this->mapper, $this->cacheDir . '/Mapper.php');
        $this->getCodeGenerator()->generateScript($this->reducer, $this->cacheDir . '/Reducer.php');

        $jobParams = array(
            $this->getHadoopStreamingJarPath(),
            'mapper' => $this->cacheDir . '/Mapper.php',
            'reducer' => $this->cacheDir . '/Reducer.php',
            'input' => $this->name . '/tasks/*',
            'output' => $this->name . '/results',
            'jobconf' => 'mapred.output.compress=false'
        );

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
     * @todo Return all results
     * @return string
     */
    private function getResultsFileHdfsPath() {
        return "{$this->getHdfsResultsDir()}/part-00000";
    }

    /**
     * @return \HadoopLib\Hadoop\Job
     */
    private function rememberResults() {
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
    public function getLastResults() {
        return $this->lastResults;
    }
}