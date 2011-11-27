<?php

namespace WordHistogramWithSorting;

class SortingMapper extends \HadoopLib\Hadoop\Job\Worker\Mapper {

    /**
     * @param string $word
     * @param mixed $count
     * @return void
     */
    protected function map($word, $count) {
        $this->emit((int) $count, $word);
    }
}