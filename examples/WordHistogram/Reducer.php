<?php

namespace WordHistogram;

class Reducer extends \Phadoop\MapReduce\Job\Worker\Reducer
{
    /**
     * @param string $key
     * @param \Traversable $values
     * @return int
     */
    protected function reduce($key, \Traversable $values) {
        $sum = 0;
        foreach ($values as $count) {
            $sum += (int) $count;
        }

        $this->emit($key, $sum);
    }

}
