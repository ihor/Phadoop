<?php
/**
 * @author Ihor Burlachenko
 */

namespace HadoopLib\Hadoop\Job\Worker;

abstract class Reducer extends \HadoopLib\Hadoop\Job\Worker {

    /**
     * @abstract
     * @param string $key
     * @param \Traversable $values
     * @return void
     */
    abstract protected function reduce($key, \Traversable $values);

    /**
     * @return void
     */
    public function handle() {
        $inputIterator = new Reducer\InputIterator($this->getReader());
        while (!$inputIterator->isIterated()) {
            $this->reduce($inputIterator->key(), $inputIterator);

            // Read to the next key
            while ($inputIterator->valid()) {
                $inputIterator->next();
            }

            $inputIterator->reset();
        }
    }
}