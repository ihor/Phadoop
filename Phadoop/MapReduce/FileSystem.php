<?php

namespace Phadoop\MapReduce;

/**
 * @see http://hadoop.apache.org/common/docs/r0.20.0/hdfs_shell.html
 */
class FileSystem
{
    /**
     * @var \Phadoop\MapReduce\Shell
     */
    private $shell;

    /**
     * @param \Phadoop\MapReduce\Shell $shell
     */
    public function __construct(Shell $shell)
    {
        $this->shell = $shell;
    }

    /**
     * @param string $cmd
     * @param string|array $args
     * @return mixed
     */
    private function exec($cmd, $args)
    {
        return $this->shell->exec("dfs -$cmd", $args);
    }

    /**
     * @param string $content Text content or path to file in local file system
     * @param string $filePath
     * @return mixed
     * @throws \Exception
     */
    public function writeToFile($content, $filePath)
    {
        if (is_file($content)) {
            return $this->exec('put', array($content, $filePath));
        }

        if (!is_string($content) && method_exists($content, '__toString')) {
            $content = $content->__toString();
        }
        
        if (is_string($content)) {
            return $this->shell->exec('printf "' . str_replace('"', '\"', str_replace('\\', '\\\\', $content)) . '" | %hadoop% dfs -put', array('-', $filePath));
        }

        throw new \Exception(sprintf('Invalid content type "%s"', is_object($content) ? get_class($content) : gettype($content)));
    }

    /**
     * Moves file from local file system to the hadoop file system
     *
     * @param string $localFilePath
     * @param string $hdfsFilePath
     * @return mixed
     */
    public function moveFromLocal($localFilePath, $hdfsFilePath)
    {
        return $this->exec('moveFromLocal', array($localFilePath, $hdfsFilePath));
    }

    /**
     * @param string $hdfsFilePath
     * @param string $localFilePath
     * @return mixed
     */
    public function copyToLocal($hdfsFilePath, $localFilePath)
    {
        return $this->exec('get', array($hdfsFilePath, $localFilePath));
    }

    /**
     * @param string $hdfsPath
     * @param bool $recursive
     * @return mixed
     */
    public function remove($hdfsPath, $recursive = false)
    {
        return $this->exec($recursive ? 'rmr' : 'rm', $hdfsPath);
    }

    /**
     * @param string $hdfsFilePath
     * @return string
     */
    public function displayFileContent($hdfsFilePath)
    {
        return $this->exec('cat', $hdfsFilePath);
    }

}
