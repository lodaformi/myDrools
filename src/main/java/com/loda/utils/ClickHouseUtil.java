package com.loda.utils;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @Author loda
 * @Date 2023/5/3 17:35
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class ClickHouseUtil {
    private static Connection conn = null;
    private static final String ckDriver = "ru.yandex.clickhouse.ClickHouseDriver";
    private static final String ckUrl = "jdbc:clickhouse://node01:8123/loda";

    public static Connection getClickHouseConnection() throws  Exception {
        Class.forName(ckDriver);
        Connection conn = DriverManager.getConnection(ckUrl);
        return conn;
    }
}




