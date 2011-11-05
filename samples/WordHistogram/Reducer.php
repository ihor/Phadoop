<?php

namespace WordHistogram;

class Reducer extends \HadoopLib\Hadoop\Job\Worker\Reducer {

    /**
     * @param string $key
     * @param \Traversable $values
     * @return int
     */
    protected function reduce($key, \Traversable $values) {
        $sum = 0;
        foreach ($values as $counts) {
            $sum += (int) $counts;
        }

        $this->emit($key, $sum);
    }
}
