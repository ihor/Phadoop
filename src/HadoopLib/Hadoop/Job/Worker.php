<?php

namespace HadoopLib\Hadoop\Job;

abstract class Worker {

    /**
     * @var \HadoopLib\Hadoop\Job\Encoder
     */
    private static $_encoder;

    /**
     * @return void
     */
    abstract public function handle();

    /**
     * @static
     * @param \HadoopLib\Hadoop\Job\Encoder $encoder
     * @return void
     */
    public static function setEncoder(Encoder $encoder) {
        self::$_encoder = $encoder;
    }

    /**
     * @return \HadoopLib\Hadoop\Job\Encoder
     */
    protected static function getEncoder() {
        if (is_null(self::$_encoder)) {
            self::$_encoder = new Encoder();
        }

        return self::$_encoder;
    }
}