<?php

namespace HadoopLib\Hadoop\Job\Worker;

abstract class Reducer extends \HadoopLib\Hadoop\Job\Worker {

    /**
     * @abstract
     * @param mix $reduced
     * @param mix $toReduce Result of map
     * @return mix
     */
    abstract protected function reduce($reduced, $toReduce);

    /**
     * @abstract
     * @return mix
     */
    abstract protected function getEmptyResult();

    /**
     * @return void
     */
    public function handle() {
        $result = $this->getEmptyResult();
        while (($toReduce = $this->read()) !== false) {
            $result = $this->reduce($result, self::getEncoder()->decode($toReduce));
        }

        echo self::getEncoder()->encode($result);
    }
}