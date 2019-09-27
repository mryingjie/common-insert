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
import com.heitaox.sql.executor.core.util.DateUtils;
import com.heitaox.sql.executor.source.rdbms.RDBMSDataSourceProperties;
import com.heitaox.sql.executor.source.rdbms.StandardSqlDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
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
        //类型|长度|最大值|最小值|是否固定位数|前缀|后缀|是否唯一|是否是纯数字
        random.put("age", "Integer|-1|100|20|false|-1|-1|false|true");
        random.put("identity_no", "String|12|-1|-1|true|-1|-1|true|true");
        random.put("createDate", "Date|-1|-1|-1|false|-1|-1|false|false");
        random.put("updateDate", "Date|-1|2019-09-01|2019-09-30|false|-1|-1|false|false");
        random.put("updateDateTime", "DateTime|-1|2019-09-30 00:00:00|2019-09-01 00:00:00|false|-1|-1|false");

        insertParam1.setRandomField(random);


        LinkedHashMap<String, String> increase = new LinkedHashMap<>();
        //类型|从几开始|位数|前缀|后缀
        increase.put("name", "String|0|-1|张三|-1");
        increase.put("id", "String|10|-1|-1|-1");
        increase.put("phone", "String|0|4|188188|-1");
        increase.put("createDateTime", "DateTime|2019-09-30 00:00:00|-1|-1|-1|-1");

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
        log.info("准备数据源");
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
                if (FieldType.DATE.equals(fieldType)) {
                    //日期类型
                    insertDateValue(sb, random, i, valueSchema, insertRule, fieldType);
                } else if (FieldType.DATETIME.equals(fieldType)) {
                    //日期时间类型
                    insertDateTimeValue(sb, random, i, valueSchema, insertRule, fieldType);
                } else {
                    //普通类型
                    insertCommonValue(sb, random, set, i, valueSchema, insertRule, fieldType);
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

    private void insertDateTimeValue(StringBuilder sb, Random random, int i, String[] valueSchema, InsertRule insertRule, FieldType fieldType) {
        if (InsertRule.CONSTANT.equals(insertRule)) {
            //常量 类型|值1,值2
            String valueArray = valueSchema[1];
            String[] values = valueArray.split(",");
            sb.append(fieldType.convert(values[random.nextInt(values.length)]));
        } else if (InsertRule.RANDOM.equals(insertRule)) {
            //类型|长度|最大值|最小值|是否固定位数|前缀|后缀|是否唯一|是否是纯数字
            String max = valueSchema[2];
            String min = valueSchema[3];
            LocalDateTime minDate = null;
            LocalDateTime maxDate = null;
            String finalDate = "";
            if (!ConStant.ANY2.equals(min)) {
                minDate = LocalDateTime.parse(min, DateUtils.dateTimeFormatter);
            }
            if (!ConStant.ANY2.equals(max)) {
                maxDate = LocalDateTime.parse(max, DateUtils.dateTimeFormatter);
            }
            if (maxDate != null && minDate != null) {
                //有最大值也有最小值
                if (maxDate.isBefore(minDate)) {
                    throw new RuntimeException("最大日期时间必须大于最小日期时间");
                }
                long between = ChronoUnit.SECONDS.between(minDate, maxDate);
                finalDate = minDate.plusSeconds(random.nextInt((int) between)).format(DateUtils.dateTimeFormatter);

            } else if (maxDate != null) {
                //有最大值
                finalDate = maxDate.plusSeconds(-random.nextInt(100 * 24 * 60 * 60)).format(DateUtils.dateTimeFormatter);

            } else if (minDate != null) {
                //有最小值
                finalDate = minDate.plusSeconds(random.nextInt(100 * 24 * 60 * 60)).format(DateUtils.dateTimeFormatter);
            } else {
                //都没有 当前日期随机增加100天以内的时间
                finalDate = LocalDateTime.now().plusSeconds(random.nextInt(100 * 24 * 60 * 60)).format(DateUtils.dateTimeFormatter);
            }
            sb.append(fieldType.convert(finalDate));
        } else {
            //自增 //类型|从几开始|位数|前缀|后缀

            String seed = valueSchema[1];
            if(ConStant.ANY2.equals(seed)){
                seed = LocalDateTime.now().format(DateUtils.dateTimeFormatter);
            }
            sb.append(fieldType.convert(LocalDateTime.parse(seed, DateUtils.dateTimeFormatter).plusSeconds(i).format(DateUtils.dateTimeFormatter)));
        }
    }


    private void insertDateValue(StringBuilder sb, Random random, int i, String[] valueSchema, InsertRule insertRule, FieldType fieldType) {
        //日期类型
        if (InsertRule.CONSTANT.equals(insertRule)) {
            //常量 类型|值1,值2
            String valueArray = valueSchema[1];
            String[] values = valueArray.split(",");
            sb.append(fieldType.convert(values[random.nextInt(values.length)]));
        } else if (InsertRule.RANDOM.equals(insertRule)) {
            //类型|长度|最大值|最小值|是否固定位数|前缀|后缀|是否唯一|是否是纯数字
            String max = valueSchema[2];
            String min = valueSchema[3];
            LocalDate minDate = null;
            LocalDate maxDate = null;
            String finalDate = "";
            if (!ConStant.ANY2.equals(min)) {
                minDate = LocalDate.parse(min, DateUtils.yyyyMMdd);
            }
            if (!ConStant.ANY2.equals(max)) {
                maxDate = LocalDate.parse(max, DateUtils.yyyyMMdd);
            }
            if (maxDate != null && minDate != null) {
                //有最大值也有最小值
                if (maxDate.isBefore(minDate)) {
                    throw new RuntimeException("最大日期必须大于最小日期");
                }
                long between = ChronoUnit.DAYS.between(minDate, maxDate);
                finalDate = minDate.plusDays(random.nextInt((int) between)).format(DateUtils.yyyyMMdd);

            } else if (maxDate != null) {
                //有最大值
                finalDate = maxDate.plusDays(-random.nextInt(100)).format(DateUtils.yyyyMMdd);

            } else if (minDate != null) {
                //有最小值
                finalDate = minDate.plusDays(random.nextInt(100)).format(DateUtils.yyyyMMdd);
            } else {
                //都没有 当前日期随机增加100天以内的时间
                finalDate = LocalDate.now().plusDays(random.nextInt(100)).format(DateUtils.yyyyMMdd);
            }
            sb.append(fieldType.convert(finalDate));
        } else {
            //自增 //类型|从几开始|位数|前缀|后缀
            String seed = valueSchema[1];
            if(ConStant.ANY2.equals(seed)){
                seed = LocalDate.now().format(DateUtils.yyyyMMdd);
            }
            sb.append(fieldType.convert(LocalDate.parse(seed, DateUtils.yyyyMMdd).plusDays(i).format(DateUtils.yyyyMMdd)));
        }
    }

    private void insertCommonValue(StringBuilder sb, Random random, Set<String> set, int i, String[] valueSchema, InsertRule insertRule, FieldType fieldType) {
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
                    randomKey = RandomUtil.createRandomKey(random, valueSchema[3], valueSchema[2], len, isNum);
                } else {
                    //不固定位数
                    if (len != -1) {
                        randomKey = RandomUtil.createRandomKey(random, valueSchema[3], valueSchema[2], random.nextInt(len) + 1, isNum);
                    } else {
                        randomKey = RandomUtil.createRandomKey(random, valueSchema[3], valueSchema[2], isNum);
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
