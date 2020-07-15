package com.zhisheng.connectors.richfun;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import javax.sql.DataSource;

public abstract class MapFuctionWithDataBase<I, O> extends RichMapFunction<I, O> {
    private static final long serialVersionUID = 20000L;
    private String driver;
    private String url;
    private String username;
    private String password;
    private DruidDataSource dataSource;
    public MapFuctionWithDataBase(String driver, String url, String username, String password) {
        this.driver = driver;
        this.url = url;
        this.username = username;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName(driver);
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setValidationQuery("select 1");
    }

    @Override
    public void close() throws Exception {
        dataSource.close();
    }

    public DataSource getDataSource(){
        return dataSource;
    }
}