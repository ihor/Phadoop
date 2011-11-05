<?php
/**
 * @author Ihor Burlachenko
 */

namespace HadoopLib\Hadoop\Job;

abstract class Worker {

    /**
     * @var \HadoopLib\Hadoop\Job\IO\Reader
     */
    private static $_reader;

    /**
     * @var \HadoopLib\Hadoop\Job\IO\Emitter
     */
    private static $_emitter;

    /**
     * @var bool
     */
    public $debug = false;

    /**
     * @var \HadoopLib\Hadoop\Job\Worker\Debugger
     */
    private $_debugger;

    /**
     * @param bool $debug
     */
    public function __construct($debug = false) {
        $this->debug = (bool) $debug;
    }

    /**
     * @static
     * @param \HadoopLib\Hadoop\Job\IO\Reader $reader
     * @return void
     */
    public static function setReader(IO\Reader $reader) {
        self::$_reader = $reader;
    }

    /**
     * @return \HadoopLib\Hadoop\Job\IO\Reader
     */
    protected static function getReader() {
        if (is_null(self::$_reader)) {
            self::$_reader = new IO\Reader();
        }

        return self::$_reader;
    }

    /**
     * @static
     * @param \HadoopLib\Hadoop\Job\IO\Emitter $emitter
     * @return void
     */
    public static function setEmitter(IO\Emitter $emitter) {
        self::$_emitter = $emitter;
    }

    /**
     * @return \HadoopLib\Hadoop\Job\IO\Emitter
     */
    private static function getEmitter() {
        if (is_null(self::$_emitter)) {
            self::$_emitter = new IO\Emitter();
        }

        return self::$_emitter;
    }

    /**
     * @static
     * @param \HadoopLib\Hadoop\Job\Worker\Debugger $debugger
     * @return \HadoopLib\Hadoop\Job\Worker
     */
    public function setDebugger(Worker\Debugger $debugger) {
        $this->_debugger = $debugger;
        return $this;
    }

    /**
     * @return \HadoopLib\Hadoop\Job\Worker\Debugger
     */
    private function getDebugger() {
        if (is_null($this->_debugger)) {
            $this->_debugger = new Worker\Debugger();
        }

        return $this->_debugger;
    }

    /**
     * @return void
     */
    abstract public function handle();
    
    /**
     * @static
     * @param string $key
     * @param mixed $value
     * @return void
     */
    protected function emit($key, $value) {
        self::getEmitter()->emit($key, $value);

        if ($this->debug) {
            $this->getDebugger()->logEmit($this, self::getEmitter()->getLast());
        }
    }
}