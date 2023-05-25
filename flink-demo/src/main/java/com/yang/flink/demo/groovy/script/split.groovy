import com.alibaba.fastjson.JSONObject
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag
import org.apache.flink.streaming.api.functions.*

class SplitProcessFunction<String> extends ProcessFunction<String, String> {

    def outputTags = [
            new OutputTag<JSONObject>("topic-A") {},
            new OutputTag<JSONObject>("topic-B") {}
    ]

    @Override
    void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
        JSONObject jsonValue = JSONObject.parseObject(value);
        if (jsonValue.get("version") == 1) {
            ctx.output(outputTags[0], value);
        } else {
            ctx.output(outputTags[1], value);
        }
    }
}
