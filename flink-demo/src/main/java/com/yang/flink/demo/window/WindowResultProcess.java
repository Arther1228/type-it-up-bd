package com.yang.flink.demo.window;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.stream.Collectors;

/**
 * 实现ProcessWindowFunction
 * abstract class ProcessWindowFunction<IN, OUT, KEY, W extends Window>
 * abstract class ProcessWindowFunction<CategoryPojo, Object, Tuple就是String类型的dateTime, TimeWindow extends Window>
 * <p>
 * 把各个分类的总价加起来，就是全站的总销量金额，
 * 然后我们同时使用优先级队列计算出分类销售的Top3，
 * 最后打印出结果，在实际中我们可以把这个结果数据存储到hbase或者redis中，以供前端的实时页面展示。
 */
public class WindowResultProcess extends ProcessWindowFunction<CategoryPojo, Object, Tuple, TimeWindow> {
    @Override
    public void process(Tuple tuple, Context context, Iterable<CategoryPojo> elements, Collector<Object> out) throws Exception {
        String dateTime = ((Tuple1<String>) tuple).f0;
        //Java中的大小顶堆可以使用优先级队列来实现
        //https://blog.csdn.net/hefenglian/article/details/81807527
        //注意:
        // 小顶堆用来计算:最大的topN
        // 大顶堆用来计算:最小的topN
        Queue<CategoryPojo> queue = new PriorityQueue<>(3,//初识容量
                //正常的排序,就是小的在前,大的在后,也就是c1>c2的时候返回1,也就是小顶堆
                (c1, c2) -> c1.getTotalPrice() >= c2.getTotalPrice() ? 1 : -1);

        //在这里我们要完成需求:
        // * -1.实时计算出11月11日00:00:00零点开始截止到当前时间的销售总额,其实就是把之前的初步聚合的price再累加!
        double totalPrice = 0D;
        double roundPrice = 0D;
        Iterator<CategoryPojo> iterator = elements.iterator();
        for (CategoryPojo element : elements) {
            double price = element.getTotalPrice();//某个分类的总销售额
            totalPrice += price;
            BigDecimal bigDecimal = new BigDecimal(totalPrice);
            roundPrice = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();//四舍五入
            // * -2.计算出各个分类的销售额top3,其实就是对各个分类的price进行排序取前3
            //注意:我们只需要top3,也就是只关注最大的前3个的顺序,剩下不管!所以不要使用全局排序,只需要做最大的前3的局部排序即可
            //那么可以使用小顶堆,把小的放顶上
            // c:80
            // b:90
            // a:100
            //那么来了一个数,和最顶上的比,如d,
            //if(d>顶上),把顶上的去掉,把d放上去,再和b,a比较并排序,保证顶上是最小的
            //if(d<=顶上),不用变
            if (queue.size() < 3) {//小顶堆size<3,说明数不够,直接放入
                queue.add(element);
            } else {//小顶堆size=3,说明,小顶堆满了,进来一个需要比较
                //"取出"顶上的(不是移除)
                CategoryPojo top = queue.peek();
                if (element.getTotalPrice() > top.getTotalPrice()) {
                    //queue.remove(top);//移除指定的元素
                    queue.poll();//移除顶上的元素
                    queue.add(element);
                }
            }
        }
        // * -3.每1秒钟更新一次统计结果,可以直接打印/sink,也可以收集完结果返回后再打印,
        //   但是我们这里一次性处理了需求1和2的两种结果,不好返回,所以直接输出!
        //对queue中的数据逆序
        //各个分类的销售额top3
        List<String> top3Result = queue.stream()
                .sorted((c1, c2) -> c1.getTotalPrice() > c2.getTotalPrice() ? -1 : 1)//逆序
                .map(c -> "(分类：" + c.getCategory() + " 销售总额：" + c.getTotalPrice() + ")")
                .collect(Collectors.toList());
        System.out.println("时间 ： " + dateTime + "  总价 : " + roundPrice + " top3:\n" + StringUtils.join(top3Result, ",\n"));
        System.out.println("-------------");

    }
}