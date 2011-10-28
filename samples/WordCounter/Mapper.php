<?php

namespace WordCounter;

class Mapper extends \HadoopLib\Hadoop\Job\Worker\Mapper {

    /**
     * @param string $content
     * @return int
     */
    protected function map($content = null)
    {
        return count(preg_split('/\s+/', trim((string) $content)));
    }
}