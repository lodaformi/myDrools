package com.loda.service;

import com.loda.utils.ClickHouseUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @Author loda
 * @Date 2023/5/3 16:50
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class ClickHouseQueryServiceImpl implements QueryService{
    @Override
    public boolean queryEventCountRangeTimes(String sql, String uid, String cid, String type, String start_time, String end_time, int counts) throws Exception {
        Connection connection = ClickHouseUtil.getClickHouseConnection();
        PreparedStatement ps = connection.prepareStatement(sql);
        //SELECT count(*) counts FROM tb_user_event WHERE uid = ? and cid = ? and type = ? and start_time >= ? AND end_time <= ? GROUP BY uid, cid
        ps.setString(1, uid);
        ps.setString(2, cid);
        ps.setString(3, type);
        ps.setString(4, start_time);
        ps.setString(5, end_time);
        ResultSet resultSet = ps.executeQuery();
        int cnt = 0;
        if (resultSet.next()) {
            cnt = resultSet.getInt("counts");
        }
        System.out.println("ck result: " + cnt);
        return cnt >= counts;
    }
}
