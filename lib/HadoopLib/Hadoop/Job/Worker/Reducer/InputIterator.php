<?php
/**
 * @author Ihor Burlachenko
 */

namespace HadoopLib\Hadoop\Job\Worker\Reducer;

/**
 * Input iterator for reducer
 * Iterates to the next input key
 */
class InputIterator implements \Iterator {

    /**
     * @var \HadoopLib\Hadoop\Job\IO\Reader
     */
    private $reader;

    /**
     * @var string
     */
    private $previousKey;

    /**
     * @var string
     */
    private $currentKey;

    /**
     * @var mixed
     */
	private $currentValue;

    /**
     * @param \HadoopLib\Hadoop\Job\IO\Reader $reader
     */
    public function __construct(\HadoopLib\Hadoop\Job\IO\Reader $reader) {
        $this->setReader($reader);
        $this->next();
        $this->reset();
	}

    /**
     * @param \HadoopLib\Hadoop\Job\IO\Reader $reader
     * @return \HadoopLib\Hadoop\Job\Worker\Reducer\InputIterator
     */
    private function setReader(\HadoopLib\Hadoop\Job\IO\Reader $reader) {
        $this->reader = $reader;
        return $this;
    }

    /**
     * Allows iterating for the current key
     *
     * @return void
     */
	public function reset() {
		$this->previousKey = $this->currentKey;
	}

    /**
     * Checks if input is processed
     *
     * @return bool
     */
	public function isIterated() {
		return is_null($this->currentKey);
	}
    
    /**
     * @return mixed
     */
    public function getValue() {
        return $this->currentValue;
    }

    /**
     * Returns current value
     * 
     * @return mixed
     */
	public function current() {
		return $this->currentValue;
	}

    /**
     * Returns current key
     * 
     * @return string
     */
	public function key() {
        if (is_null($this->currentKey)) {
            return null;
        }

		return (string) $this->currentKey;
	}

    /**
     * @return void
     */
	public function next() {
		$this->currentKey = null;
        $this->currentValue = null;

        if (false !== $input = $this->reader->read()) {
            $this->currentKey = $input->getKey();
            $this->currentValue = $input->getValue();
        }
	}

    /**
     * @return void
     */
	public function rewind() {}

    /**
     * Iterator is valid until we read another key
     *
     * @return bool
     */
	public function valid() {
        return $this->currentKey == $this->previousKey;
	}
}
