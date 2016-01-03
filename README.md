Phadoop
=======

Phadoop allows you to write map/reduce tasks for Hadoop in PHP. I created it to give a techtalk about Hadoop in the company I worked in. It is not ready for production use yet but can help you to play with Hadoop in PHP.

Installation
------------
Define the following requirement in your composer.json file:
```
"require": {
    "ihor/phadoop": "0.1.x-dev"
}
```
or simply execute the following in the command line:
```
composer require ihor/phadoop
```

Usage
-----
```php
class Mapper extends \Phadoop\MapReduce\Job\Worker\Mapper
{
    protected function map($key, $value)
    {
        $this->emit('wordsNumber', count(preg_split('/\s+/', trim((string) $value))));
    }

}

class Reducer extends \Phadoop\MapReduce\Job\Worker\Reducer
{
    protected function reduce($key, \Traversable $values)
    {
        $result = 0;
        foreach ($values as $value) {
            $result += (int) $value;
        }

        $this->emit($key, $result);
    }
}

$mr = new \Phadoop\MapReduce('<path-to-hadoop>');

$job = $mr->createJob('WordCounter', 'Temp')
    ->setMapper(new Mapper())
    ->setReducer(new Reducer())
    ->clearData()
    ->addTask('Hello World')
    ->addTask('Hello Hadoop')
    ->putResultsTo('Temp/Results.txt')
    ->run();

echo $job->getLastResults();
```

You can find more examples in the [examples](https://github.com/ihor/Phadoop/tree/master/examples) directory.
