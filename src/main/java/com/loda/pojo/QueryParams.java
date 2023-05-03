package com.loda.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author loda
 * @Date 2023/5/3 16:53
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class QueryParams {
    private String sql;

    private String uid;

    private String cid;

    //mysql数据库中时间类型转换到java中要看使用场景找到对应类型
    private String start_time;

    private String end_time;

    private Integer counts;
}
