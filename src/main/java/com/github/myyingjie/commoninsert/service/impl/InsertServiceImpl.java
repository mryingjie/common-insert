package com.github.myyingjie.commoninsert.service.impl;

import com.alibaba.fastjson.JSON;
import com.github.myyingjie.commoninsert.bean.ConStant;
import com.github.myyingjie.commoninsert.bean.FieldType;
import com.github.myyingjie.commoninsert.bean.InsertRule;
import com.github.myyingjie.commoninsert.bean.InsertParam;
import com.github.myyingjie.commoninsert.service.InsertService;
import com.github.myyingjie.commoninsert.util.RandomUtil;
import com.heitaox.sql.executor.SQLExecutor;
import com.heitaox.sql.executor.core.entity.Tuple2;
import com.heitaox.sql.executor.source.rdbms.RDBMSDataSourceProperties;
import com.heitaox.sql.executor.source.rdbms.StandardSqlDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/**
 * created by Yingjie Zheng at 2019-09-27 11:12
 */
@Service
@Slf4j
public class InsertServiceImpl implements InsertService {

    public static void main(String[] args) {
        InsertParam insertParam1 = new InsertParam();
        insertParam1.setHost("localhost");
        insertParam1.setPort(3306);
        insertParam1.setDatabase("tests");
        insertParam1.setTableName("user");
        insertParam1.setNum(100);
        insertParam1.setUserName("root");
        insertParam1.setPassword("zheng");

        LinkedHashMap<String, String> random = new LinkedHashMap<>();
        //类型|长度|最大值|最小值|是否固定位数|前缀|后缀|是否唯一
        random.put("age", "Integer|-1|100|20|false|-1|-1|false");
        random.put("identity_no", "String|12|-1|-1|true|-1|-1|true");
        insertParam1.setRandomField(random);


        LinkedHashMap<String, String> increase = new LinkedHashMap<>();
        //类型|从几开始|位数|前缀|后缀
        increase.put("name", "String|0|-1|张三|-1");
        increase.put("id", "String|10|-1|-1|-1");
        increase.put("phone", "String|0|4|188188|-1");
        insertParam1.setIncrease(increase);


        LinkedHashMap<String, String> constant = new LinkedHashMap<>();
        constant.put("constant", "String|星宿老仙");
        constant.put("sex", "String|男,女");
        insertParam1.setConstantField(constant);
        System.out.println(JSON.toJSONString(insertParam1));
    }


    /**
     * //类型|长度|最大值|最小值|是否固定位数|前缀|后缀|是否唯一|是否是纯数字
     * random.put("age","Integer|3|100|20|false|-1|-1|false|false");
     * random.put("identity_no", "String|12|-1|-1|true|-1|-1|true|true");
     * <p>
     * LinkedHashMap<String, String> increase = new LinkedHashMap<>();
     * //类型|从几开始|位数|前缀|后缀      //自增到位数限制就会截取
     * increase.put("name","String|0|-1|张三|-1");
     * increase.put("id", "String|10|-1|-1|-1");
     * increase.put("phone", "String|0|4|188188|-1");
     * insertParam1.setIncrease(increase);
     * <p>
     * //类型|值1,值2
     * insertParam1.setRandomField(random);
     * LinkedHashMap<String, Object> constant = new LinkedHashMap<>();
     * constant.put("constant","String|星宿老仙" );
     * constant.put("sex","String|男,女");
     */

    @Override
    public int insert(InsertParam insertParam) throws IOException, SQLException {

        //准备数据源
        RDBMSDataSourceProperties rdbmsDataSourceProperties = prepareDataSource(insertParam);
        SQLExecutor.SQLExecutorBuilder builder = new SQLExecutor.SQLExecutorBuilder();
        SQLExecutor sqlExecutor = builder
                .putDataSource(insertParam.getTableName(), new StandardSqlDataSource(rdbmsDataSourceProperties))
                .build();
        //拼接sql
        String sql = spliceSql(insertParam);

        //插入数据
        int i = sqlExecutor.executeInsert(sql);
        return i;
    }

    private String spliceSql(InsertParam insertParam) {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(insertParam.getTableName()).append(" (");
        // 插入字段
        List<Tuple2<String[], InsertRule>> fieldValueIndex = new ArrayList<>();

        LinkedHashMap<String, String> constantField = insertParam.getConstantField();
        for (Map.Entry<String, String> entry : constantField.entrySet()) {
            String field = entry.getKey();
            sb.append(field).append(", ");
            fieldValueIndex.add(new Tuple2<>(entry.getValue().split(ConStant.SEPARATOR), InsertRule.CONSTANT));
        }

        LinkedHashMap<String, String> randomField = insertParam.getRandomField();
        for (Map.Entry<String, String> entry : randomField.entrySet()) {
            String field = entry.getKey();
            sb.append(field).append(", ");
            fieldValueIndex.add(new Tuple2<>(entry.getValue().split(ConStant.SEPARATOR), InsertRule.RANDOM));
        }

        LinkedHashMap<String, String> increaseField = insertParam.getIncrease();
        for (Map.Entry<String, String> entry : increaseField.entrySet()) {
            String field = entry.getKey();
            sb.append(field).append(", ");
            fieldValueIndex.add(new Tuple2<>(entry.getValue().split(ConStant.SEPARATOR), InsertRule.INCREASE));
        }
        sb.deleteCharAt(sb.lastIndexOf(","));
        sb.append(") ").append("VALUES");

        //插入值
        int num = insertParam.getNum();
        Random random = new Random();
        Set<String> set = new HashSet<>();

        for (int i = 0; i < num; i++) {
            sb.append("( ");
            for (Tuple2<String[], InsertRule> valueIndex : fieldValueIndex) {
                String[] valueSchema = valueIndex.getV1();
                InsertRule insertRule = valueIndex.getV2();
                FieldType fieldType = FieldType.getByValue(valueSchema[0]);
                if (InsertRule.CONSTANT.equals(insertRule)) {
                    //常量 类型|值1,值2
                    String valueArray = valueSchema[1];
                    String[] values = valueArray.split(",");
                    sb.append(fieldType.convert(values[random.nextInt(values.length)]));

                } else if (InsertRule.RANDOM.equals(insertRule)) {
                    //随机值  //类型|长度|最大值|最小值|是否固定位数|前缀|后缀|是否唯一|是否是纯数字
                    String prefix = valueSchema[5];
                    String suffix = valueSchema[6];
                    boolean isUnique = Boolean.parseBoolean(valueSchema[7]);
                    // if (!(ConStant.ANY2.equals(prefix) && ConStant.ANY2.equals(suffix))) {
                    //     // 有前后缀
                    //     if (!fieldType.equals(FieldType.STRING)) {
                    //         throw new RuntimeException("有前后缀的值必须是String类型");
                    //     }
                    // }
                    if (ConStant.ANY2.equals(prefix)) {
                        prefix = "";
                    }
                    if (ConStant.ANY2.equals(suffix)) {
                        suffix = "";
                    }

                    int len = Integer.parseInt(valueSchema[1]);

                    boolean isFixedLength = Boolean.parseBoolean(valueSchema[4]);
                    boolean isNum = Boolean.parseBoolean(valueSchema[8]);
                    String randomKey;
                    do {
                        if (isFixedLength) {
                            //固定位数 不能是数字类型
                            randomKey = RandomUtil.createRandomKey(random, valueSchema[3], valueSchema[2], len,isNum);
                        } else {
                            //不固定位数
                            if (len != -1) {
                                randomKey = RandomUtil.createRandomKey(random, valueSchema[3], valueSchema[2], random.nextInt(len) + 1,isNum);
                            }else {
                                randomKey = RandomUtil.createRandomKey(random, valueSchema[3], valueSchema[2],isNum);
                            }
                        }
                    } while (set.contains(randomKey) && isUnique);
                    set.add(randomKey);
                    sb.append(fieldType.convert(prefix + randomKey + suffix));

                } else {
                    //自增值
                    //类型|从几开始|位数|前缀|后缀
                    String prefix = valueSchema[3];
                    String suffix = valueSchema[4];
                    // if (!(ConStant.ANY2.equals(prefix) && ConStant.ANY2.equals(suffix))) {
                    //     // 有前后缀
                    //     if (!fieldType.equals(FieldType.STRING)) {
                    //         throw new RuntimeException("有前后缀的值必须是String类型");
                    //     }
                    // }
                    if (ConStant.ANY2.equals(prefix)) {
                        prefix = "";
                    }
                    if (ConStant.ANY2.equals(suffix)) {
                        suffix = "";
                    }
                    long seed = Long.parseLong(valueSchema[1]);
                    String len = valueSchema[2];
                    String str = seed + i + "";
                    if (!ConStant.ANY2.equals(len)) {
                        str = RandomUtil.appendHeadZero(str, Integer.parseInt(len));
                    }
                    sb.append(fieldType.convert(prefix + str + suffix));
                }
                sb.append(",");

            }
            sb.deleteCharAt(sb.lastIndexOf(","));
            sb.append(" ) ").append(",");
        }
        sb.deleteCharAt(sb.lastIndexOf(","));
        String sql = sb.toString();
        log.info("execute sql :{}", sql);
        return sql;
    }

    private RDBMSDataSourceProperties prepareDataSource(InsertParam insertParam) {
        RDBMSDataSourceProperties dataSourceProperties = new RDBMSDataSourceProperties();
        String host = insertParam.getHost();
        int port = insertParam.getPort();
        String database = insertParam.getDatabase();
        dataSourceProperties.setUrl("jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=utf8&autoReconnect=true&failOverReadOnly=false&autoReconnect=true&failOverReadOnly=false&serverTimezone=GMT%2B8");
        dataSourceProperties.setUsername(insertParam.getUserName());
        dataSourceProperties.setPassword(insertParam.getPassword());
        dataSourceProperties.setDriverClass("com.mysql.cj.jdbc.Driver");
        dataSourceProperties.setInitialSize(5);
        dataSourceProperties.setTestOnReturn(false);
        dataSourceProperties.setMinEvictableIdleTimeMillis(50000L);
        return dataSourceProperties;
    }


}
