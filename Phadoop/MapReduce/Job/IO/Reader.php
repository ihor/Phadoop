<?php
/**
 * @author Ihor Burlachenko
 */

namespace Phadoop\MapReduce\Job\IO;

class Reader
{
    /**
     * @return \Phadoop\MapReduce\Job\IO\Data\Input|false
     */
    public static function read()
    {
        $line = fgets(STDIN);
        if ($line !== false) {
            return Data\Input::createFromString(trim($line));
        }

        return false;
    }

}
