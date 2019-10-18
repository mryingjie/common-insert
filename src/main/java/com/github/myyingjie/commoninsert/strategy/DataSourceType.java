package com.github.myyingjie.commoninsert.strategy;

import com.github.myyingjie.commoninsert.bean.DataSourceProperties;
import com.github.myyingjie.commoninsert.exception.BizException;
import com.heitaox.sql.executor.source.DataSource;
import com.heitaox.sql.executor.source.file.ExcelDataSource;
import com.heitaox.sql.executor.source.nosql.ElasticsearchDataSource;
import com.heitaox.sql.executor.source.nosql.MongoDataSource;
import com.heitaox.sql.executor.source.nosql.MongoDataSourceProperties;
import com.heitaox.sql.executor.source.rdbms.RDBMSDataSourceProperties;
import com.heitaox.sql.executor.source.rdbms.StandardSqlDataSource;
import com.mongodb.ServerAddress;
import org.apache.http.HttpHost;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * created by Yingjie Zheng at 2019-09-29 09:39
 */
public enum DataSourceType {

    MYSQL("mysql") {
        @Override
        public DataSource createDataSource(DataSourceProperties insertParam) throws Exception {
            try{
                RDBMSDataSourceProperties dataSourceProperties = new RDBMSDataSourceProperties();
                String host = insertParam.getHost();
                int port = insertParam.getPort() == 0 ? 3306 : insertParam.getPort();
                String database = insertParam.getDatabase();
                dataSourceProperties.setUrl("jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=utf8&autoReconnect=true&failOverReadOnly=false&autoReconnect=true&failOverReadOnly=false&serverTimezone=GMT%2B8");
                dataSourceProperties.setUsername(insertParam.getUserName());
                dataSourceProperties.setPassword(insertParam.getPassword());
                dataSourceProperties.setDriverClass("com.mysql.cj.jdbc.Driver");
                dataSourceProperties.setInitialSize(5);
                dataSourceProperties.setTestOnReturn(false);
                dataSourceProperties.setMinEvictableIdleTimeMillis(50000L);
                return new StandardSqlDataSource(dataSourceProperties);
            }catch (Throwable e){
                throw new BizException("数据源创建失败！");
            }

        }
    },

    ES("es") {
        @Override
        public DataSource createDataSource(DataSourceProperties param) {
            try{
                int port = param.getPort() == 0 ? 9200 : param.getPort();
                String[] split = param.getHost().split(",");
                List<HttpHost> hosts = Stream.of(split)
                        .map(host -> new HttpHost(host, port, "http"))
                        .collect(Collectors.toList());

                return new ElasticsearchDataSource(hosts);
            }catch (Throwable e){
                throw new BizException("数据源创建失败！");
            }

        }
    },

    MONGO("mongo") {
        @Override
        public DataSource createDataSource(DataSourceProperties param) {
            try {
                int port = param.getPort() == 0 ? 27017 : param.getPort();
                String[] split = param.getHost().split(",");
                List<ServerAddress> hosts = Stream.of(split)
                        .map(host -> new ServerAddress(host, port))
                        .collect(Collectors.toList());

                if(param.getUserName()!=null){
                    MongoDataSourceProperties mongoDataSourceProperties = new MongoDataSourceProperties();
                    mongoDataSourceProperties.setUser(param.getUserName());
                    mongoDataSourceProperties.setPassword(param.getPassword().toCharArray());
                    mongoDataSourceProperties.setDbName(param.getDatabase());
                    mongoDataSourceProperties.setServerAddress(hosts);
                    return new MongoDataSource(mongoDataSourceProperties);
                }

                return new MongoDataSource(hosts, param.getDatabase());
            }catch (Throwable e){
                throw new BizException("数据源创建失败！");
            }

        }
    },

    EXCEL("excel") {
        @Override
        public DataSource createDataSource(DataSourceProperties param) {
            try {
                return new ExcelDataSource(param.getFilePath(), null);

            }catch (Throwable e){
                throw new BizException("数据源创建失败！");
            }
        }
    };


    private String type;

    DataSourceType(String type) {
        this.type = type;
    }

    public String getType(){
        return this.type;
    }

    public abstract DataSource createDataSource(DataSourceProperties param) throws Exception;

    public static DataSourceType getByType(String type) {
        for (DataSourceType value : DataSourceType.values()) {
            if (value.type.equals(type)) {
                return value;
            }

        }
        throw new BizException("not support this type of data source:[{" + type + "}]");
    }

}
