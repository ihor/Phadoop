<?php
/**
 * @author Ihor Burlachenko
 */

namespace HadoopLib\Hadoop\Job;

class Debugger {

    /**
     * @static
     * @param \HadoopLib\Hadoop\Job\Worker $worker
     * @param \HadoopLib\Hadoop\Job\IO\Data\Output $output
     * @return void
     */
    public static function logEmit(Worker $worker, IO\Data\Output $output) {
        error_log(get_class($worker) . ': ' . $output);
    }
}
