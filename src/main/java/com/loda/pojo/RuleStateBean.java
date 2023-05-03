package com.loda.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.kie.api.runtime.KieSession;

/**
 * @Author loda
 * @Date 2023/5/3 14:09
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleStateBean {
    private Integer id;

    private KieSession kieSession;

    private String sql;

    private String start_time;

    private String end_time;

    private Integer counts;
}
