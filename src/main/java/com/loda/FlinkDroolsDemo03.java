package com.loda;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.loda.pojo.*;
import com.loda.service.ClickHouseQueryServiceImpl;
import com.loda.service.QueryService;
import com.loda.utils.FlinkSourceUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.utils.KieHelper;

import java.util.Map;

/**
 * @Author loda
 * @Date 2023/5/3 10:11
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 * 数据源两个：①canal采集mysql ②socket输入
 * flink计算
 * 触发规则，并且动态查询clinkhouse
 */
public class FlinkDroolsDemo03 {
    public static void main(String[] args) throws Exception {
        FlinkSourceUtils.env.setParallelism(1);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);
        DataStream<String> kafkaRulesStream = FlinkSourceUtils.createKafkaStream(parameterTool, "droolsRules", SimpleStringSchema.class);

//        kafkaRulesStream.print();
        SingleOutputStreamOperator<DroolsRuleBean> rulesBeanStream = kafkaRulesStream.process(new ProcessFunction<String, DroolsRuleBean>() {
            @Override
            public void processElement(String value, ProcessFunction<String, DroolsRuleBean>.Context ctx, Collector<DroolsRuleBean> out) throws Exception {
                try {//防止转换时出现异常
                    JSONObject jsonObject = JSON.parseObject(value);
                    String type = jsonObject.getString("type");
                    if ("INSERT".equals(type) || "UPDATE".equals(type)) {
                        JSONArray dataArray = jsonObject.getJSONArray("data");
                        for (int i = 0; i < dataArray.size(); i++) {
                            DroolsRuleBean droolsRuleBean = dataArray.getObject(i, DroolsRuleBean.class);
                            out.collect(droolsRuleBean);
                        }
                    }
                } catch (Exception e) {
                    //TODO
                }
            }
        });

        //声明广播状态
        MapStateDescriptor<Integer, RuleStateBean> mapStateDescriptor =
                new MapStateDescriptor<>("drools-rules-map", Types.INT, TypeInformation.of(new TypeHint<RuleStateBean>() {
                }));
        //将rulesBeanStream广播出去
        BroadcastStream<DroolsRuleBean> ruleBeanBroadcastStream = rulesBeanStream.broadcast(mapStateDescriptor);

        //event Steam
        DataStream<String> kafkaEventStream = FlinkSourceUtils.createKafkaStream(parameterTool, "events", SimpleStringSchema.class);
        KeyedStream<Tuple3<String, String, String>, String> eventKeyedStream = kafkaEventStream.map(new MapFunction<String, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> map(String value) throws Exception {
                        String[] split = value.split(",");
                        return Tuple3.of(split[0], split[1], split[2]);
                    }
                })
                //以用户ID为key
                .keyBy(value -> value.f0);

        eventKeyedStream.connect(ruleBeanBroadcastStream).process(new KeyedBroadcastProcessFunction<String, Tuple3<String, String, String>, DroolsRuleBean, Tuple3<String, String, String>>() {
            private transient MapState<Tuple2<String, String>, Integer> mapState;
            private QueryService queryService;

            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<Tuple2<String, String>, Integer> stateDescriptor =
                        new MapStateDescriptor<>("event-map-state", Types.TUPLE(Types.STRING, Types.STRING), Types.INT);
                mapState = getRuntimeContext().getMapState(stateDescriptor);

                queryService = new ClickHouseQueryServiceImpl();
            }

            @Override
            public void processElement(Tuple3<String, String, String> value, KeyedBroadcastProcessFunction<String, Tuple3<String, String, String>, DroolsRuleBean,
                    Tuple3<String, String, String>>.ReadOnlyContext ctx, Collector<Tuple3<String, String, String>> out) throws Exception {
                String uid = value.f0;
                String cid = value.f1;
                String eventType = value.f2;
                Tuple2<String, String> key = Tuple2.of(cid, eventType);

                Integer cnt = mapState.get(key);
                if (cnt == null) {
                    cnt = 0;
                }
                mapState.put(key, ++cnt);
                System.out.println("key: "+ key.toString() +" cnt: " +cnt);
                Event event = new Event(eventType, cnt, false);

                Iterable<Map.Entry<Integer, RuleStateBean>> entries = ctx.getBroadcastState(mapStateDescriptor).immutableEntries();
                for (Map.Entry<Integer, RuleStateBean> entry : entries) {
                    RuleStateBean bean = entry.getValue();
                    KieSession kieSession = bean.getKieSession();
                    //准备数据
                    QueryParams queryParams = new QueryParams(bean.getSql(), uid, cid, bean.getStart_time(), bean.getEnd_time(), bean.getCounts());
                    DroolsRulesParam droolsRulesParam = new DroolsRulesParam(event, queryService, queryParams, false);
                    //插入数据
                    kieSession.insert(droolsRulesParam);
                    //将数据应用到规则中
                    kieSession.fireAllRules();
                    //如果规则触发，此处的值会改动
                    if (droolsRulesParam.getIsHit()) {
                        out.collect(Tuple3.of(uid, "发送优惠卷", "满1000打骨折"));
                    }
                }
            }

            @Override
            public void processBroadcastElement(DroolsRuleBean value, KeyedBroadcastProcessFunction<String, Tuple3<String, String, String>,
                                DroolsRuleBean, Tuple3<String, String, String>>.Context ctx, Collector<Tuple3<String, String, String>> out) throws Exception {
                //从ctx中获取状态
                BroadcastState<Integer, RuleStateBean> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                Integer id = value.getId();
                Integer status = value.getStatus();

                if (status == 1 || status == 2) {
                    KieHelper kieHelper = new KieHelper();
                    kieHelper.addContent(value.getCode(), ResourceType.DRL);
                    KieSession kieSession = kieHelper.build().newKieSession();
                    RuleStateBean ruleStateBean = new RuleStateBean(id, kieSession, value.getSql(), value.getStart_time(), value.getEnd_time(), value.getCounts());
                    broadcastState.put(id, ruleStateBean);
                }else { ///status == 3
                    broadcastState.remove(id);
                }
                broadcastState.immutableEntries().forEach(x -> System.out.println(x.getValue()));
            }
        }).print();

        FlinkSourceUtils.env.execute();
    }
}
