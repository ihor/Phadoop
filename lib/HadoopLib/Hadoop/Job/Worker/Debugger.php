<?php
/**
 * @author Ihor Burlachenko
 */

namespace HadoopLib\Hadoop\Job\Worker;

use \HadoopLib\Hadoop\Job\Worker;
use \HadoopLib\Hadoop\Job\IO\Data\Output;

class Debugger {

    /**
     * @static
     * @param \HadoopLib\Hadoop\Job\Worker $emitter
     * @param \HadoopLib\Hadoop\Job\IO\Data\Output $output
     * @return void
     */
    public static function logEmit(Worker $emitter, Output $output) {
        error_log(get_class($emitter) . ': ' . $output);
    }
}
