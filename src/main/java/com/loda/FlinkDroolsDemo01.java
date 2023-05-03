package com.loda;

import com.loda.pojo.Event;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.utils.KieHelper;

import java.io.File;
import java.nio.charset.StandardCharsets;

/**
 * @Author loda
 * @Date 2023/5/2 23:18
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 * 从socket读取数据，flink计算，触发drools规则
 */
public class FlinkDroolsDemo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //用户ID，商品分类ID，事件类型
        //u1001,c201,view
        //一个用户，在一天之内，浏览某个分类商品的次数大于2就触发相应事件
        DataStreamSource<String> dataStream = env.socketTextStream("node03", 7777);
        KeyedStream<Tuple3<String, String, String>, String> keyedStream = dataStream.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple3.of(split[0], split[1], split[2]);
            }
        })
            //按照用户ID进行分组
            .keyBy(tp3 -> tp3.f0);

        //key是用户ID（字符串类型），输入是tuple3，输出是tuple3
        keyedStream.process(new KeyedProcessFunction<String, Tuple3<String, String, String>, Tuple3<String, String, String>>() {
            //此处是为了计算用户访问商品分类使用了事件类型的次数，前面已经对用户ID进行了分组，此处的数据都是某一个用户产生的
            //所以只要将商品分类ID（CID）和事件类型作为key，统计出现的次数（value）即可。
            private transient MapState<Tuple2<String, String>, Integer> mapState;
            private transient KieSession kieSession;
            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<Tuple2<String, String>, Integer> mapStateDescriptor =
                        new MapStateDescriptor<>("category-event-count", Types.TUPLE(Types.STRING, Types.STRING), Types.INT);
                mapState = getRuntimeContext().getMapState(mapStateDescriptor);

                //Drools的工具类，应用规则，创建匹配规则的会话
                KieHelper kieHelper = new KieHelper();

                //将Drools的规则文件转成字符串
                String ruleString = FileUtils.readFileToString(new File("rules/first-demo.drl"), StandardCharsets.UTF_8);

                //添加规则
                kieHelper.addContent(ruleString, ResourceType.DRL);

                //创建规则匹配的会话（将用户传入的数据和之前添加的规则进行匹配）
                kieSession = kieHelper.build().newKieSession();
            }

            @Override
            public void processElement(Tuple3<String, String, String> value, KeyedProcessFunction<String, Tuple3<String, String, String>, Tuple3<String, String, String>>.Context ctx, Collector<Tuple3<String, String, String>> out) throws Exception {
                String uid = value.f0;
                String cid = value.f1;
                String eventType = value.f2;

                Tuple2<String, String> key = Tuple2.of(cid, eventType);
                Integer count =mapState.get(key);
                //如果key不存在，表示key是第一次访问，将count置为0
                if (count == null) {
                    count = 0;
                }
                //key已经存在过，count自增1，并且更新状态
                mapState.put(key, ++count);
                //System.out.println("count "+count);

                Event event = new Event(eventType, count, false);
                //传入用户的数据
                kieSession.insert(event);

                //将数据应用到所有的规则
                kieSession.fireAllRules();
                if (event.isHit()) {
                    out.collect(Tuple3.of(uid, "发优惠券", "满100打骨折"));
                }
                //System.out.println("-----"+ event.isHit());
            }
        }).print();
        

        //env exec
        env.execute();
    }
}
