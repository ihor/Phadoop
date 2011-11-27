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
            $this->fileSystem->writeToFile(Job\IO\Encoder::encode($task), $taskHdfsFilePath);
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
        file_put_contents($encodedTaskLocalFilePath, Job\IO\Encoder::encode($content));
        return $encodedTaskLocalFilePath;
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
         */

        $jobParams = array(
            $this->getHadoopStreamingJarPath(),
            'mapper' => $this->cacheDir . '/Mapper.php',
            'reducer' => $this->cacheDir . '/Reducer.php',
            'input' => $this->name . '/tasks/*',
            'output' => $this->name . '/results',
            /*'file' => $this->cacheDir . '/Mapper.php',
            'file' => $this->cacheDir . '/Reducer.php',*/
            'jobconf' => 'mapred.output.compress=false'
        );

        if ($this->hasCombiner()) {
            if ($this->combiner->isEqualTo($this->reducer)) {
                $jobParams['combiner'] = $this->cacheDir . '/Reducer.php';
            }
            else {
                $this->getCodeGenerator()->generateScript($this->combiner, $this->cacheDir . '/Combiner.php');
                $jobParams['combiner'] = $this->cacheDir . '/Combiner.php';
            }
        }

        $this->shell->exec('jar', $jobParams);

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
     * @return string
     */
    private function displayResults() {
        echo "Results:\n";
        $this->fileSystem->displayFileContent($this->getResultsFileHdfsPath());
    }

    /**
     * @todo Return all results
     * @return string
     */
    private function getResultsFileHdfsPath() {
        return "{$this->getHdfsResultsDir()}/part-00000";
    }
}