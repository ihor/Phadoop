<?php

namespace Phadoop\MapReduce\Job\IO;

class Encoder
{
    /**
     * @param mixed $data
     * @return string
     */
    public static function encode($data)
    {
        return json_encode($data);
    }

    /**
     * @param string $data
     * @return mixed
     */
    public static function decode($data)
    {
        return json_decode(trim($data), true);
    }

}