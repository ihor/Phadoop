<?php

namespace WordCounter;

class Reducer extends \Phadoop\MapReduce\Job\Worker\Reducer
{
    /**
     * @param string $key
     * @param \Traversable $values
     * @return int
     */
    protected function reduce($key, \Traversable $values)
    {
        $result = 0;
        foreach ($values as $value) {
            $result += (int) $value;
        }

        $this->emit($key, $result);
    }

}
