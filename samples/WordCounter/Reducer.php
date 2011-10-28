<?php

namespace WordCounter;

class Reducer extends \HadoopLib\Hadoop\Job\Worker\Reducer {

    /**
     * @param int $result
     * @param int $wordNumber
     * @return int
     */
    protected function reduce($result, $wordNumber)
    {
        return (int) $result + (int) $wordNumber;
    }

    /**
     * @return int
     */
    protected function getEmptyResult()
    {
        return 0;
    }
}
