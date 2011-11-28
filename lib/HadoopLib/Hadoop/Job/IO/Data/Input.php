<?php
/**
 * @author Ihor Burlachenko
 */

namespace HadoopLib\Hadoop\Job\IO\Data;

class Input extends \HadoopLib\Hadoop\Job\IO\Data {

    /**
     * @param string $inputString
     * @return \HadoopLib\Hadoop\Job\IO\Input
     */
    public static function createFromString($inputString) {
        $inputStringParts = explode(self::$delimiter, trim($inputString));

        if (count($inputStringParts) == 1) {
            return new self(self::DEFAULT_KEY, self::getEncoder()->decode($inputStringParts[0]));
        }

        return new self($inputStringParts[0], self::getEncoder()->decode($inputStringParts[1]));
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
}
