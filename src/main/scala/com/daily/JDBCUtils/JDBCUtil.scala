package com.daily.JDBCUtils

import java.util.Properties
import javax.sql.DataSource
import com.alibaba.druid.pool.DruidDataSourceFactory

import java.sql.{Connection, PreparedStatement}

class JDBCUtil {
    var dataSource: DataSource = init()
    
    def init(): DataSource = {
        val properties = new Properties()
        properties.setProperty("driverClassName", "org.postgresql.Driver")
        properties.setProperty("url", "jdbc:postgresql://121.36.222.1:5432/daily?currentSchema=daily")
        properties.setProperty("username", "gaussdb")
        properties.setProperty("password", "Group7pwd@")
        properties.setProperty("maxActive", "50")
        DruidDataSourceFactory.createDataSource(properties)
    }
    
    def getConnection: Connection = {
        dataSource.getConnection
    }
    
    def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Int = {
        var rtn = 0
        var pstmt: PreparedStatement = null
        try {
            connection.setAutoCommit(false)
            pstmt = connection.prepareStatement(sql)
            
            if (params != null && params.length > 0) {
                for (i <- params.indices) {
                    pstmt.setObject(i + 1, params(i))
                }
            }
            rtn = pstmt.executeUpdate()
            connection.commit()
            pstmt.close()
        } catch {
            case e: Exception => e.printStackTrace()
        }
        rtn
    }
    
    def isExist(connection: Connection, sql: String, params: Array[Any]): Boolean = {
        var flag: Boolean = false
        var pstmt: PreparedStatement = null
        try {
            pstmt = connection.prepareStatement(sql)
            for (i <- params.indices) {
                pstmt.setObject(i + 1, params(i))
            }
            flag = pstmt.executeQuery().next()
            pstmt.close()
        } catch {
            case e: Exception => e.printStackTrace()
        }
        flag
    }
}
