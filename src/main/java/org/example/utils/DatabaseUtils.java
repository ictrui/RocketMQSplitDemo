package org.example.utils;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatabaseUtils {
    private static final Logger LOGGER = Logger.getLogger(DatabaseUtils.class.getName());

    public static String executeQuery(String sql, String databaseUrl, String username, String password) throws Exception {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            // 加载驱动程序
            Class.forName("com.mysql.cj.jdbc.Driver");

            // 建立数据库连接
            connection = DriverManager.getConnection(databaseUrl, username, password);
            LOGGER.log(Level.INFO, "Database connected successfully.");

            // 创建SQL语句执行器
            statement = connection.createStatement();

            // 执行SQL查询语句
            resultSet = statement.executeQuery(sql);

        } catch (ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "Error loading driver: " + e.getMessage(), e);
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error executing SQL query: " + e.getMessage(), e);
        }

        return resultSetToString(resultSet);
    }

    public static String resultSetToString(ResultSet resultSet) throws Exception {
        StringBuilder stringBuilder = new StringBuilder();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                String columnValue = resultSet.getString(i);
                stringBuilder.append(columnName).append(": ").append(columnValue).append(", ");
            }
            // Remove the last comma and space and add a newline for each row
            stringBuilder.setLength(stringBuilder.length() - 2);
            stringBuilder.append("\n");
        }

        return stringBuilder.toString();
    }
}
