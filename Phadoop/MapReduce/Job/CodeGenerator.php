<?php
/**
 * @author Ihor Burlachenko
 */

namespace Phadoop\MapReduce\Job;

class CodeGenerator
{
    /**
     * @var string
     */
    private $templatesPath;

    /**
     * @var string
     */
    private $phadoopPath;

    /**
     * @todo Replace this with a better solution
     * @var string
     */
    private $universalClassLoaderPath;

    /**
     * @throws \Exception
     */
    public function __construct()
    {
        if (!class_exists('\Symfony\Component\ClassLoader\UniversalClassLoader')) {
            throw new \Exception('Symfony UniversalClassLoader is needed');
        }

        $classReflection = new \ReflectionClass('\Symfony\Component\ClassLoader\UniversalClassLoader');
        $this->universalClassLoaderPath = $classReflection->getFileName();

        $thisReflection = new \ReflectionClass($this);
        $thisPath = $thisReflection->getFileName();

        // Assuming that templates are in the same directory as the CodeGenerator
        $this->templatesPath = substr($thisPath, 0, strpos($thisPath, 'CodeGenerator.php'));
        $this->phadoopPath = substr($thisPath, 0, strpos($thisPath, '/Phadoop/'));
    }

    /**
     * @todo Add restoring worker state with reflection
     * @param \Phadoop\MapReduce\Job\Worker $worker
     * @param string $outputFile
     * @return void
     */
    public function generateScript(Worker $worker, $outputFile)
    {
        $script = file_get_contents("{$this->templatesPath}/CodeGenerator/Worker.php.tpl");

        $workerReflectionClass = new \ReflectionClass($worker);
        $workerClassName = $workerReflectionClass->getName();
        $projectNamespaceName = $workerReflectionClass->getNamespaceName();
        if (false !== $slashPos = strpos('\\', $projectNamespaceName)) {
            $projectNamespaceName = substr($projectNamespaceName, 0, $slashPos);
        }

        $workerFilePath = $workerReflectionClass->getFileName();
        $projectNamespacePath = substr($workerFilePath, 0, strpos($workerFilePath, "/$projectNamespaceName"));

        $script = str_replace('%PhadoopMapReduceDebug%', defined('PHADOOP_MAPREDUCE_DEBUG') && PHADOOP_MAPREDUCE_DEBUG ? 'true' : 'false', $script);
        $script = str_replace('%UniversalClassLoaderPath%', $this->universalClassLoaderPath, $script);
        $script = str_replace('%PhadoopPath%', $this->phadoopPath, $script);
        $script = str_replace('%ProjectNamespaceName%', $projectNamespaceName, $script);
        $script = str_replace('%ProjectNamespacePath%', $projectNamespacePath, $script);
        $script = str_replace('%ProjectWorkerClassName%', $workerClassName, $script);

        file_put_contents($outputFile, $script);
        chmod($outputFile, 0755); // Make the script executable
    }

}
