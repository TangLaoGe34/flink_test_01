package utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class FlatMapFunc implements FlatMapFunction<String,Tuple2<String,Integer>> {
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
        String[] split = value.split(" ");

        for (String word : split) {
            out.collect(Tuple2.of(word,1));
        }
    }
}
