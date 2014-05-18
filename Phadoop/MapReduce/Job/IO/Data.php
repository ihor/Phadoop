<?php

namespace Phadoop\MapReduce\Job\IO;

abstract class Data {

    /**
     * @const string
     */
    const DEFAULT_KEY = 0;

    /**
     * Default key-value delimiter for the Hadoop streaming
     * http://hadoop.apache.org/common/docs/r0.15.2/streaming.html
     *
     * @var string
     */
    protected static $delimiter = "\t";

    /**
     * @var \Phadoop\MapReduce\Job\IO\Encoder
     */
    private static $_encoder;

    /**
     * @static
     * @param string $delimiter
     * @return void
     */
    public static function setDelimiter($delimiter) {
        self::$delimiter = (string) $delimiter;
    }

    /**
     * @static
     * @param \Phadoop\MapReduce\Job\IO\Encoder $encoder
     * @return void
     */
    public static function setEncoder(Encoder $encoder) {
        self::$_encoder = $encoder;
    }

    /**
     * @return \Phadoop\MapReduce\Job\IO\Encoder
     */
    protected static function getEncoder() {
        if (is_null(self::$_encoder)) {
            self::$_encoder = new Encoder();
        }

        return self::$_encoder;
    }

    /**
     * @var string
     */
    protected $key;

    /**
     * @var mixed
     */
    protected $value;

    /**
     * @param string $key
     * @param mixed $value
     */
    protected function __construct($key, $value) {
        if (is_null($key)) {
            $key = self::DEFAULT_KEY;
        }

        $this->key = (string) $key;
        $this->value = $value;
    }
}