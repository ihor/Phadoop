Assuming that the project is located in ~/Development/PHPHadoopLib/

1. Copy tasks to HDFS
bin/hadoop dfs -copyFromLocal ~/Development/PHPHadoopLib/samples/DummyWordHistogram/Tasks/MapReduceTutorial.txt /Jobs/DummyWordHistogram/Tasks

2. Run job
bin/hadoop jar contrib/streaming/hadoop-streaming-0.20.203.0.jar -mapper ~/Development/PHPHadoopLib/samples/DummyWordHistogram/Mapper.php -reducer ~/Development/PHPHadoopLib/samples/DummyWordHistogram/Reducer.php -input /Jobs/DummyWordHistogram/Tasks/* -output /Jobs/DummyWordHistogram/Output

3. View results
bin/hadoop dfs -cat /Jobs/DummyWordHistogram/Output/part-00000

(c) http://hadoop.apache.org/common/docs/r0.17.2/mapred_tutorial.html