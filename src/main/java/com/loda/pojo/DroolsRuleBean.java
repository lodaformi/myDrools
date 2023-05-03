package com.loda.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @Author loda
 * @Date 2023/5/3 11:25
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DroolsRuleBean {
    private Integer id;

    private String name;

    private String code;

    //增加sql字段
    private String sql;

    //mysql数据库中时间类型转换到java中要看使用场景找到对应类型
    private String start_time;

    private String end_time;

    private Integer counts;

    private Integer status;
}
