/**
 * @desc: target_name为待截取的字段
 */
import com.alibaba.fastjson.JSONObject
import org.apache.flink.api.common.functions.MapFunction


class SubStrMapFunction implements MapFunction<String, String> {

    def fieldName = "target_name"
    def startIndex = 0
    def length = 2

    @Override
    String map(String value) throws Exception {
        JSONObject jsonValue = JSONObject.parseObject(value);
        String fieldValueStr = (String) jsonValue.get(fieldName);
        String substring = fieldValueStr.substring(startIndex, length);
        jsonValue.put(fieldName, substring);
        String result = JSONObject.toJSONString(jsonValue);
        return result;
    }

}