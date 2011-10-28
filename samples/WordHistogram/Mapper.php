<?php

namespace WordHistogram;

class Mapper extends \HadoopLib\Hadoop\Job\Worker\Mapper {

    /**
     * @param string $content
     * @return array
     */
    protected function map($content = null)
    {
        $content = strtolower(trim($content));
        $words = preg_split('/\W/', $content, 0, PREG_SPLIT_NO_EMPTY);

        $counters = array();
        foreach ($words as $word) {
            if (!array_key_exists($word, $counters)) {
                $counters[$word] = 0;
            }
            $counters[$word] += 1;
        }

        return $counters;
    }
}