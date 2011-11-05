<?php
/**
 * @author Ihor Burlachenko
 */

namespace HadoopLib\Hadoop\Job\IO;

class Emitter {

    /**
     * @var \HadoopLib\Hadoop\Job\IO\Data\Output
     */
    private $last;

    /**
     * @param string $key
     * @param mixed $value
     * @return void
     */
	public function emit($key, $value) {
        $output = Data\Output::create($key, $value);
        echo $output . "\n";

        $this->last = $output;
    }

    /**
     * @return \HadoopLib\Hadoop\Job\IO\Data\Output
     */
    public function getLast() {
        return $this->last;
    }
}
