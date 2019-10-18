package com.github.myyingjie.commoninsert.config;

import com.alibaba.fastjson.JSON;
import com.github.myyingjie.commoninsert.bean.DataSourceProperties;
import com.github.myyingjie.commoninsert.strategy.DataSourceType;
import com.github.myyingjie.commoninsert.util.JsonFormatTool;
import com.github.myyingjie.commoninsert.util.DatasourceFileUtil;
import com.heitaox.sql.executor.SQLExecutor;
import com.heitaox.sql.executor.core.entity.Tuple2;
import com.heitaox.sql.executor.source.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * created by Yingjie Zheng at 2019-10-10 17:41
 */
@Configuration
@Slf4j
public class SQLExecutorConfig{

    public Map<String, DataSource> dataSourceMap;

    public Map<String, DataSourceProperties> dataSourcePropertiesMap = new ConcurrentHashMap<>();

    public static final String DATA_SOURCE_FILE_NAME = "datasource.json";

    @Bean
    public SQLExecutor sqlExecutor() {

        SQLExecutor.SQLExecutorBuilder builder = new SQLExecutor.SQLExecutorBuilder();
        try {
            String s = DatasourceFileUtil.readResourcesJsonFile(DATA_SOURCE_FILE_NAME);
            List<DataSourceProperties> dataSourceProperties = JSON.parseArray(s, DataSourceProperties.class);
            for (DataSourceProperties dataSourceProperty : dataSourceProperties) {
                String type = dataSourceProperty.getType();
                DataSource dataSource = DataSourceType.getByType(type).createDataSource(dataSourceProperty);
                builder.putDataSource(dataSourceProperty.getDatabase(), dataSource);
                dataSourceProperty.setPersistenced(true);
                dataSourcePropertiesMap.put(dataSourceProperty.getDatabase(), dataSourceProperty);
            }
        } catch (Exception e) {
            log.warn("init SQLExecutor warn,some dataSource from datasource.json wile be not init", e);
        }
        SQLExecutor sqlExecutor = builder.build();

        try {
            Class<?> aclass = Class.forName(sqlExecutor.getClass().getName());
            Field dataSources = aclass.getDeclaredField("dataSources");
            dataSources.setAccessible(true);
            dataSourceMap = (Map) dataSources.get(sqlExecutor);
            log.info("init datasource from datasource.json succeed!!!{}", JSON.toJSONString(dataSourceMap.keySet()));
        } catch (Exception e) {
            log.error("获取SQLExecutor的datasources失败", e);
        }
        log.info("init SQLExecutor completed!!");

        return sqlExecutor;
    }


    public synchronized void persistence(String database) throws IOException {
        DataSourceProperties dataSourceProperties = dataSourcePropertiesMap.get(database);

        if (dataSourceProperties != null) {
            String s = DatasourceFileUtil.readResourcesJsonFile(DATA_SOURCE_FILE_NAME);
            dataSourceProperties.setPersistenced(true);
            List<DataSourceProperties> list = JSON.parseArray(s, DataSourceProperties.class);
            list.add(dataSourceProperties);
            String json = JSON.toJSONString(list);
            try {
                DatasourceFileUtil.write(DATA_SOURCE_FILE_NAME, JsonFormatTool.formatJson(json));
            } catch (IOException e) {
                dataSourceProperties.setPersistenced(false);
                throw e;
            }
        }
    }

    public static void main(String[] args) {
        Tuple2<Object, Object> tuple = new Tuple2<>("dada", 123);
        System.out.println(JSON.toJSONString(tuple));
    }

    public synchronized void persistence() throws IOException {
        String json = "[\n" +
                "]";
        if (dataSourcePropertiesMap.size() != 0) {
            json = JSON.toJSONString(dataSourcePropertiesMap.values());
        }
        DatasourceFileUtil.write(DATA_SOURCE_FILE_NAME, JsonFormatTool.formatJson(json));
    }

    public synchronized void persistenceDelete(String database) throws IOException {
        String s = DatasourceFileUtil.readResourcesJsonFile(DATA_SOURCE_FILE_NAME);
        List<DataSourceProperties> list = JSON.parseArray(s, DataSourceProperties.class);
        list.removeIf(next -> next.getDatabase().equalsIgnoreCase(database));
        String json = "[\n" +
                "]";
        if (list.size() != 0) {
            json = JSON.toJSONString(list);
        }
        DatasourceFileUtil.write(DATA_SOURCE_FILE_NAME, JsonFormatTool.formatJson(json));


    }

    public Set<String> databaseList() {
        return dataSourcePropertiesMap.keySet();
    }
}
