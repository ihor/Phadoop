<?php
/**
 * @author Ihor Burlachenko
 */

namespace HadoopLib\Hadoop\Job\IO;

class Reader {

    /**
     * @return \HadoopLib\Hadoop\Job\IO\Data\Input|false
     */
    public static function read() {
        $line = fgets(STDIN);
        if ($line !== false) {
            return Data\Input::createFromString(trim($line));
        }

        return false;
    }
}
