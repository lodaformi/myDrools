package com.loda.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author loda
 * @Date 2023/5/2 22:36
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Event {
    private String type;

    private int count;

    private boolean hit;
}
