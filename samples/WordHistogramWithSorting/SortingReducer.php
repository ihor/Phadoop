<?php

namespace WordHistogramWithSorting;

class SortingReducer extends \Phadoop\MapReduce\Job\Worker\Reducer {

    /**
     * @param string $count
     * @param \Traversable $words
     * @return int
     */
    protected function reduce($count, \Traversable $words) {
        foreach ($words as $word) {
            $this->emit($word, (int) $count);
        }
    }
}
