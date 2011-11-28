<?php
/**
 * @author Ihor Burlachenko
 */

namespace HadoopLib\Hadoop\Job\IO\Data;

class Output extends \HadoopLib\Hadoop\Job\IO\Data {

    /**
     * @static
     * @param string $key
     * @param mixed $value
     * @return \HadoopLib\Hadoop\Job\IO\Output
     */
    public static function create($key, $value) {
        return new self($key, $value);
    }

    /**
     * @return string
     */
    public function getKey() {
        return $this->key;
    }

    /**
     * @return mix
     */
    public function getValue() {
        return $this->value;
    }

    /**
     * @return string
     */
    public function __toString() {
        return $this->key . self::$delimiter . self::getEncoder()->encode($this->value);
    }
}
