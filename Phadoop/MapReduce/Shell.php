<?php
/**
 * @author Ihor Burlachenko
 */

namespace Phadoop\MapReduce;

class Shell {

    /**
     * Path to the Hadoop
     *
     * @var string
     */
    private $hadoopPath;

    /**
     * @param string $hadoopPath Path
     */
    public function __construct($hadoopPath) {
        $this->hadoopPath = (string) $hadoopPath;
    }

    /**
     * @return string
     */
    public function getHadoopPath() {
        return $this->hadoopPath;
    }

    /**
     * @param string $cmd
     * @param array|string $args
     * @return mix
     */
    public function exec($cmd, $args) {
        return system("{$this->prepareCmd($cmd)} {$this->prepareCmdArgs($args)}");
    }

    /**
     * @param string $cmd
     * @return string
     */
    private function prepareCmd($cmd)
    {
        $result = (string) $cmd;
        if (strpos($result, '%hadoop%') === false) {
            $result = "{$this->hadoopPath}/bin/hadoop $result";
        }
        else {
            $result = str_replace('%hadoop%', "{$this->hadoopPath}/bin/hadoop", $result);
        }

        return $result;
    }

    /**
     * @param string|array $args
     * @return string
     */
    private function prepareCmdArgs($args)
    {
        if (!is_array($args)) {
            return (string) $args;
        }

        $result = '';
        foreach ($args as $arg => $value) {
            if (!is_int($arg)) {
                $arg = (string) $arg;
                $result .= " -$arg";
            }

            $value = (string) $value;
            $result .= " $value";
        }

        return trim($result);
    }
}