<?php

namespace WordHistogram;

class Mapper extends \HadoopLib\Hadoop\Job\Worker\Mapper {

    /**
     * @param string $key
     * @param mixed $value
     * @return void
     */
    protected function map($key, $value) {
        $content = strtolower(trim($value));
        $words = preg_split('/\W/', $content, 0, PREG_SPLIT_NO_EMPTY);

        $histogram = array();
        foreach ($words as $word) {
            if (!array_key_exists($word, $histogram)) {
                $histogram[$word] = 0;
            }

            $histogram[$word]++;
        }

        foreach ($histogram as $word => $count) {
            $this->emit($word, $count);
        }
    }
}