package com.loda.pojo;

import com.loda.service.QueryService;
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
public class DroolsRulesParam {
    private Event event;

    private QueryService queryService;

    private QueryParams queryParams;

    private Boolean isHit;
}
