package com.loda;

import com.loda.pojo.Event;
import org.apache.commons.io.FileUtils;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.utils.KieHelper;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @Author loda
 * @Date 2023/5/2 22:32
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Hello01DroolsDemo {
    public static void main(String[] args) throws IOException {
        //Drools的工具类，应用规则，创建匹配规则的会话
        KieHelper kieHelper = new KieHelper();

        //将Drools的规则文件转成字符串
        String ruleString = FileUtils.readFileToString(new File("rules/first-demo.drl"), StandardCharsets.UTF_8);

        //添加规则
        kieHelper.addContent(ruleString, ResourceType.DRL);

        //创建规则匹配的会话（将用户传入的数据和之前添加的规则进行匹配）
        KieSession kieSession = kieHelper.build().newKieSession();

        //创建数据
        Event event = new Event("view", 3, false);
        //传入用户的数据
//        kieSession.insert("hello");
        kieSession.insert(event);

        //将数据应用到所有的规则
        kieSession.fireAllRules();

        System.out.println(event.isHit());
    }
}
