<?php
/**
 * @author Ihor Burlachenko
 */

namespace Phadoop\MapReduce\Job;

class Debugger
{
    /**
     * @static
     * @param \Phadoop\MapReduce\Job\Worker $worker
     * @param \Phadoop\MapReduce\Job\IO\Data\Output $output
     * @return void
     */
    public static function logEmit(Worker $worker, IO\Data\Output $output)
    {
        error_log(get_class($worker) . ': ' . $output);
    }

}
