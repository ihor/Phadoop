<?php

namespace WordCounter;

class Mapper extends \HadoopLib\Hadoop\Job\Worker\Mapper {

    /**
     * @param string $key
     * @param mixed $value
     * @return void
     */
    protected function map($key, $value) {
        $this->emit('wordsNumber', count(preg_split('/\s+/', trim((string) $value))));
    }
}