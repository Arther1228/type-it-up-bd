import com.alibaba.fastjson.JSONObject
import org.apache.flink.api.common.functions.FilterFunction

class FieldFilterFunction implements FilterFunction<String> {

    def fieldName = "version"

    @Override
    boolean filter(String value) throws Exception {
        JSONObject jsonValue = JSONObject.parseObject(value);
        def flag
        if (jsonValue.get(fieldName) == 2) {
            flag = true
        }
        return flag
    }
}