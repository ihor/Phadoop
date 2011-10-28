<?php

namespace HadoopLib\Hadoop\Job\Worker;

abstract class Mapper extends \HadoopLib\Hadoop\Job\Worker {

    /**
     * @abstract
     * @return mix
     */
    abstract protected function map();

    /**
     * @return void
     */
    public function handle() {
        while (($task = fgets(STDIN)) !== false) {
            echo self::getEncoder()->encode(call_user_func_array(
                array($this, 'map'),
                array(self::getEncoder()->decode($task))
            ));
        }
    }
}