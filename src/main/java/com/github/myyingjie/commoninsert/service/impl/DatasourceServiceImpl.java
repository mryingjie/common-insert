package com.github.myyingjie.commoninsert.service.impl;

import com.github.myyingjie.commoninsert.bean.*;
import com.github.myyingjie.commoninsert.config.SQLExecutorConfig;
import com.github.myyingjie.commoninsert.exception.BizException;
import com.github.myyingjie.commoninsert.service.DatsourceService;
import com.github.myyingjie.commoninsert.strategy.DataSourceType;
import com.github.myyingjie.commoninsert.util.ReflectUtil;
import com.heitaox.sql.executor.source.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * created by Yingjie Zheng at 2019-09-27 11:12
 */
@Service
@Slf4j
public class DatasourceServiceImpl implements DatsourceService {


    @Autowired
    private SQLExecutorConfig sqlExecutorConfig;


    @Override
    public List<DataSourcePropertiesVo> queryDatasource(int status) {
        Collection<DataSourceProperties> values = sqlExecutorConfig.dataSourcePropertiesMap.values();
        return values.stream().filter(properties -> {
            if (status == 0) {
                return false;
            } else if (status == 1) {
                //只有数据库
                return !DataSourceType.EXCEL.getType().equalsIgnoreCase(properties.getType());
            } else if (status == 2) {
                //只有文件
                return DataSourceType.EXCEL.getType().equalsIgnoreCase(properties.getType());
            } else {
                //全查
                return true;
            }

        }).sorted(Comparator.comparing(DataSourceProperties::getType))
                .map(dataSourceProperties -> {
                    DataSourcePropertiesVo dataSourcePropertiesVo = new DataSourcePropertiesVo();
                    BeanUtils.copyProperties(dataSourceProperties, dataSourcePropertiesVo);
                    ReflectUtil.setDefaultValue(dataSourcePropertiesVo, dataSourceProperties.getClass());
                    return dataSourcePropertiesVo;
                })
                .collect(Collectors.toList());
    }

    @Override
    public synchronized void deleteDatasource(String database) throws IOException {
        sqlExecutorConfig.dataSourcePropertiesMap.remove(database);
        DataSource remove = sqlExecutorConfig.dataSourceMap.get(database);
        List<String> tableName = new ArrayList<>();
        for (Map.Entry<String, DataSource> entry : sqlExecutorConfig.dataSourceMap.entrySet()) {
            if (entry.getValue().equals(remove)) {
                tableName.add(entry.getKey());
            }
        }
        for (String s : tableName) {
            sqlExecutorConfig.dataSourceMap.remove(s);
        }

        //持久化
        persistenceDelete(database);
    }

    @Override
    public synchronized void updateDatasource(DataSourceProperties dataSourceProperties) throws Exception {
        String database = dataSourceProperties.getDatabase();
        DataSourceProperties original = sqlExecutorConfig.dataSourcePropertiesMap.get(database);
        DataSource remove;
        if (!dataSourceProperties.equals(original)) {
            //更新
            remove = sqlExecutorConfig.dataSourceMap.get(database);
            List<String> tableName = new ArrayList<>();
            for (Map.Entry<String, DataSource> entry : sqlExecutorConfig.dataSourceMap.entrySet()) {
                if (entry.getValue().equals(remove)) {
                    tableName.add(entry.getKey());
                }
            }
            DataSource dataSource = DataSourceType.getByType(dataSourceProperties.getType()).createDataSource(dataSourceProperties);
            for (String s : tableName) {
                sqlExecutorConfig.dataSourceMap.put(s, dataSource);
            }

            sqlExecutorConfig.dataSourceMap.put(database, dataSource);

            dataSourceProperties.setPersistenced(false);
            sqlExecutorConfig.dataSourcePropertiesMap.put(database, dataSourceProperties);

        }
    }



    @Override
    public synchronized void addDatasource(DataSourceProperties dataSourceProperties) throws Exception {
        if (sqlExecutorConfig.dataSourcePropertiesMap.containsKey(dataSourceProperties.getDatabase())) {
            throw new BizException(dataSourceProperties.getDatabase() + " is already exists ，please delete it first!!");
        }
        DataSource dataSource = DataSourceType.getByType(dataSourceProperties.getType()).createDataSource(dataSourceProperties);
        sqlExecutorConfig.dataSourcePropertiesMap.put(dataSourceProperties.getDatabase(), dataSourceProperties);
        sqlExecutorConfig.dataSourceMap.put(dataSourceProperties.getDatabase(), dataSource);
    }

    @Override
    public synchronized void persistence(String database) throws IOException {
        sqlExecutorConfig.persistence(database);
    }

    @Override
    public Set<String> queryDatabase() {
        return sqlExecutorConfig.databaseList();
    }

    public void persistenceDelete(String database) throws IOException {
        sqlExecutorConfig.persistenceDelete(database);
    }




}
