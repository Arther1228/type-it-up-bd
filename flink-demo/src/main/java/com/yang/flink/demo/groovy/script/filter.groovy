/**
 * 如果filter函数返回false会被过滤
 */

import com.alibaba.fastjson.JSONObject
import org.apache.flink.api.common.functions.FilterFunction

class FieldFilterFunction implements FilterFunction<String> {

    def fieldName = "version"

    @Override
    boolean filter(String value) throws Exception {
        JSONObject jsonValue = JSONObject.parseObject(value);
        def flag = true
        if (jsonValue.get(fieldName) == 2) {
            flag = false
        }
        return flag
    }
}