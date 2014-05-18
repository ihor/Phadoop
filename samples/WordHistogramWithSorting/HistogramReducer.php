<?php

namespace WordHistogramWithSorting;

class HistogramReducer extends \Phadoop\MapReduce\Job\Worker\Reducer
{
    /**
     * @param string $word
     * @param \Traversable $counts
     * @return int
     */
    protected function reduce($word, \Traversable $counts) {
        $sum = 0;
        foreach ($counts as $count) {
            $sum += (int) $count;
        }

        $this->emit($word, $sum);
    }

}
