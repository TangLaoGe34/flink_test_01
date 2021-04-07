package flink01;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.FlatMapFunc;

public class WordCountUnbounded {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(1);

        DataStreamSource<String> streamSource = environment.socketTextStream("192.168.213.101", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = streamSource.flatMap(new FlatMapFunc());

        KeyedStream<Tuple2<String, Integer>, String> keyBy = flatMap.keyBy(x -> x.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyBy.sum(1);

        sum.print();

        environment.execute("tang");

    }
}
