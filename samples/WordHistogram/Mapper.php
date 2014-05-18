<?php

namespace WordHistogram;

class Mapper extends \Phadoop\MapReduce\Job\Worker\Mapper {

    /**
     * @param string $key
     * @param mixed $value
     * @return void
     */
    protected function map($key, $value) {
        $content = strtolower(trim($value));
        $words = preg_split('/\W/', $content, 0, PREG_SPLIT_NO_EMPTY);

        foreach ($words as $word) {
            $this->emit($word, 1);
        }
    }
}