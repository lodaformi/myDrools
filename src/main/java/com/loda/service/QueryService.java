package com.loda.service;

/**
 * @Author loda
 * @Date 2023/5/3 16:49
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public interface QueryService {
    boolean queryEventCountRangeTimes(String sql, String uid, String cid, String type,
                                      String start_time, String end_time, int counts) throws  Exception;
}
