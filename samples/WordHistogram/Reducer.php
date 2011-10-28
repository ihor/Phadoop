<?php

namespace WordHistogram;

class Reducer extends \HadoopLib\Hadoop\Job\Worker\Reducer {

    /**
     * @param array $histogram
     * @param array $wordCounters
     * @return array
     */
    protected function reduce($histogram, $wordCounters)
    {
        foreach ($wordCounters as $word => $count) {
            if (!array_key_exists($word, $histogram)) {
                $histogram[$word] = 0;
            }

            $histogram[$word] += $count;
        }

        return $histogram;
    }

    /**
     * @return array
     */
    protected function getEmptyResult()
    {
        return array();
    }
}
