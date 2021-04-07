package flink01;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import utils.FlatMapFunc;

public class WordCountBounded {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.println("testgit");
        environment.setParallelism(1);

        System.out.println("test brach");
        DataStreamSource<String> source = environment.readTextFile("src/main/resources/word.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = source.flatMap(new FlatMapFunc());

        KeyedStream<Tuple2<String, Integer>, String> keyBy = flatMap.keyBy(x -> x.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyBy.sum(1);

        sum.print();

        environment.execute("tang");


    }
}
