package com.yang.flink.demo.accumulator;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * IntCounter也好，LongCount也好，适合处理有限流，对于kafka这种，还是要用windows才行
 */
public class Test {

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<String> datasource = environment.fromElements("a", "b", "c", "d", "e");

		datasource.flatMap(new RichFlatMapFunction<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -8327402858378106440L;

			// 创建计数器
			private IntCounter counter = new IntCounter();

			@Override
			public void open(Configuration parameters) throws Exception {
				// 注册累加器
				getRuntimeContext().addAccumulator("words", counter);
				super.open(parameters);
			}

			@Override
			public void flatMap(String value, Collector<String> out) throws Exception {
				out.collect(value);
				// 使用累加器,处理数据成功一次，累加一次
				this.counter.add(1);
			}
		}).writeAsText("D:\\aaa\\bbb");

		JobExecutionResult rs = environment.execute("Test");
		Integer nums = rs.getAccumulatorResult("words");
		System.out.println("一共处理成功的单词数量是:" + nums);
	}
}