<?php
/**
 * @author Ihor Burlachenko
 */

namespace Phadoop\MapReduce\Job\IO\Data;

class Output extends \Phadoop\MapReduce\Job\IO\Data
{
    /**
     * @static
     * @param string $key
     * @param mixed $value
     * @return \Phadoop\MapReduce\Job\IO\Data\Output
     */
    public static function create($key, $value)
    {
        return new self($key, $value);
    }

    /**
     * @return string
     */
    public function getKey()
    {
        return $this->key;
    }

    /**
     * @return mixed
     */
    public function getValue()
    {
        return $this->value;
    }

    /**
     * @return string
     */
    public function __toString()
    {
        return $this->key . self::$delimiter . self::getEncoder()->encode($this->value);
    }

}
