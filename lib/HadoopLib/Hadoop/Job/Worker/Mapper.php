<?php
/**
 * @author Ihor Burlachenko
 */

namespace HadoopLib\Hadoop\Job\Worker;

abstract class Mapper extends \HadoopLib\Hadoop\Job\Worker {

    /**
     * @abstract
     * @param string $key
     * @param mixed $value
     * @return void
     */
    abstract protected function map($key, $value);

    /**
     * @return void
     */
    public function handle() {
        while (($input = self::getReader()->read()) !== false) {
            $this->map($input->getKey(), $input->getValue());
        }
    }
}