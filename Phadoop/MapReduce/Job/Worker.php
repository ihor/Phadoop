<?php
/**
 * @author Ihor Burlachenko
 */

namespace Phadoop\MapReduce\Job;

abstract class Worker {

    /**
     * @var \Phadoop\MapReduce\Job\IO\Reader
     */
    private static $_reader;

    /**
     * @var \Phadoop\MapReduce\Job\IO\Emitter
     */
    private static $_emitter;

    /**
     * @var \Phadoop\MapReduce\Job\Worker\Debugger
     */
    private $_debugger;

    /**
     * @static
     * @param \Phadoop\MapReduce\Job\IO\Reader $reader
     * @return void
     */
    public static function setReader(IO\Reader $reader) {
        self::$_reader = $reader;
    }

    /**
     * @return \Phadoop\MapReduce\Job\IO\Reader
     */
    protected static function getReader() {
        if (is_null(self::$_reader)) {
            self::$_reader = new IO\Reader();
        }

        return self::$_reader;
    }

    /**
     * @static
     * @param \Phadoop\MapReduce\Job\IO\Emitter $emitter
     * @return void
     */
    public static function setEmitter(IO\Emitter $emitter) {
        self::$_emitter = $emitter;
    }

    /**
     * @return \Phadoop\MapReduce\Job\IO\Emitter
     */
    private static function getEmitter() {
        if (is_null(self::$_emitter)) {
            self::$_emitter = new IO\Emitter();
        }

        return self::$_emitter;
    }

    /**
     * @static
     * @param \Phadoop\MapReduce\Job\Debugger $debugger
     * @return \Phadoop\MapReduce\Job\Worker
     */
    public function setDebugger(Debugger $debugger) {
        $this->_debugger = $debugger;
        return $this;
    }

    /**
     * @return \Phadoop\MapReduce\Job\Debugger
     */
    private function getDebugger() {
        if (is_null($this->_debugger)) {
            $this->_debugger = new Debugger();
        }

        return $this->_debugger;
    }

    /**
     * @return bool
     */
    private function isInDebugMode() {
        return defined('PHADOOP_MAPREDUCE_DEBUG') && PHADOOP_MAPREDUCE_DEBUG;
    }

    /**
     * @param \Phadoop\MapReduce\Job\Worker $worker
     * @return bool
     */
    public function isEqualTo(Worker $worker) {
        return get_class($this) === get_class($worker);
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

        if ($this->isInDebugMode()) {
            $this->getDebugger()->logEmit($this, self::getEmitter()->getLast());
        }
    }
}