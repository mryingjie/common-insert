package com.github.myyingjie.commoninsert.service.impl;

import com.alibaba.fastjson.JSON;
import com.github.myyingjie.commoninsert.bean.ConStant;
import com.github.myyingjie.commoninsert.bean.DataSourceProperties;
import com.github.myyingjie.commoninsert.bean.InsertParam;
import com.github.myyingjie.commoninsert.bean.InsertRule;
import com.github.myyingjie.commoninsert.config.SQLExecutorConfig;
import com.github.myyingjie.commoninsert.service.InsertService;
import com.github.myyingjie.commoninsert.strategy.DataSourceType;
import com.github.myyingjie.commoninsert.strategy.FieldType;
import com.github.myyingjie.commoninsert.util.RandomUtil;
import com.heitaox.sql.executor.SQLExecutor;
import com.heitaox.sql.executor.core.entity.Tuple2;
import com.heitaox.sql.executor.core.util.DateUtils;
import com.heitaox.sql.executor.source.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * created by Yingjie Zheng at 2019-10-12 17:23
 */
@Service
@Slf4j
public class InsertServiceImpl implements InsertService {

    @Autowired
    private SQLExecutor sqlExecutor;

    @Autowired
    private SQLExecutorConfig sqlExecutorConfig;


    /**
     * //类型|长度|最大值|最小值|是否固定位数|前缀|后缀|是否唯一|字符策略(0:任意 1:纯数字 2:纯字母 3:纯汉字 4:数字+字母 5:数字+汉字 6:字母+汉字)|小数位数
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
    public int insert(InsertParam insertParam) throws Exception {
        String database = insertParam.getDatabase();
        if (!sqlExecutorConfig.dataSourcePropertiesMap.containsKey(insertParam.getDatabase())) {
            throw new RuntimeException("no data source of " + insertParam.getDatabase() + " find,Please configure the data source first ");
        }
        DataSourceProperties dataSourceProperties = sqlExecutorConfig.dataSourcePropertiesMap.get(insertParam.getDatabase());
        Map<String, DataSource> dataSourceMap = sqlExecutorConfig.dataSourceMap;
        if (dataSourceMap.containsKey(database)) {
            if (!dataSourceMap.containsKey(insertParam.getTableName())) {
                //数据源有 但是没有和当前表相关联 先将表和库关联
                dataSourceMap.put(insertParam.getTableName(), dataSourceMap.get(database));
            }
        } else {
            //第一次加载的数据源 需要初始化并放入数据源的缓存池中
            log.info("准备数据源,type:{}", dataSourceProperties.getType());
            DataSource dataSource = DataSourceType
                    .getByType(dataSourceProperties.getType())
                    .createDataSource(dataSourceProperties);
            dataSourceMap.put(dataSourceProperties.getDatabase(), dataSource);
            dataSourceMap.put(insertParam.getTableName(), dataSource);
        }

        //拼接sql
        String sql = spliceSql(insertParam);
        //插入数据
        return sqlExecutor.executeInsert(sql);
    }

    private String spliceSql(InsertParam insertParam) {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(insertParam.getTableName()).append(" (");
        // 插入字段
        List<Tuple2<String[], InsertRule>> fieldValueIndex = new ArrayList<>();

        LinkedHashMap<String, String> constantField = insertParam.getConstant();
        if (constantField != null) {
            for (Map.Entry<String, String> entry : constantField.entrySet()) {
                String field = entry.getKey();
                sb.append(field).append(", ");
                fieldValueIndex.add(new Tuple2<>(entry.getValue().split(ConStant.SEPARATOR), InsertRule.CONSTANT));
            }
        }

        LinkedHashMap<String, String> randomField = insertParam.getRandom();
        if (randomField != null) {
            for (Map.Entry<String, String> entry : randomField.entrySet()) {
                String field = entry.getKey();
                sb.append(field).append(", ");
                fieldValueIndex.add(new Tuple2<>(entry.getValue().split(ConStant.SEPARATOR), InsertRule.RANDOM));
            }
        }

        LinkedHashMap<String, String> increaseField = insertParam.getIncrease();
        if (increaseField != null) {
            for (Map.Entry<String, String> entry : increaseField.entrySet()) {
                String field = entry.getKey();
                sb.append(field).append(", ");
                fieldValueIndex.add(new Tuple2<>(entry.getValue().split(ConStant.SEPARATOR), InsertRule.INCREASE));
            }
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
                try {
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
                } catch (Exception ex) {
                    log.error("data format exception error message:{} ,param:{}", ex.getMessage(), JSON.toJSONString(valueIndex), ex);
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
            if (ConStant.ANY2.equals(seed)) {
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
            if (ConStant.ANY2.equals(seed)) {
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
            //随机值  //类型|长度|最大值|最小值|是否固定位数|前缀|后缀|是否唯一|字符策略(0:任意 1:纯数字 2:纯字母 3:纯汉字 4:数字+字母 5:数字+汉字 6:字母+汉字)|几位小数
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
            int characterStrategy = Integer.parseInt(valueSchema[8]);
            String randomKey;
            do {
                if (FieldType.DOUBLE.equals(fieldType) || FieldType.DECIMAL.equals(fieldType)) {
                    //小数
                    if (valueSchema[3].equals(ConStant.ANY2)) {
                        valueSchema[3] = "0";
                    }
                    if (valueSchema[2].equals(ConStant.ANY2)) {
                        valueSchema[2] = "100";
                    }
                    BigDecimal min = new BigDecimal(valueSchema[3]);
                    BigDecimal max = new BigDecimal(valueSchema[2]);

                    BigDecimal bigDecimal = new BigDecimal(random.nextDouble()).multiply(max.subtract(min)).add(min).setScale(Integer.parseInt(valueSchema[9]), BigDecimal.ROUND_DOWN);
                    randomKey = bigDecimal.toString();
                } else {
                    if (isFixedLength) {
                        //固定位数
                        randomKey = RandomUtil.createRandomKey(random, valueSchema[3], valueSchema[2], len, characterStrategy);
                    } else {
                        //不固定位数
                        if (len != -1) {
                            randomKey = RandomUtil.createRandomKey(random, valueSchema[3], valueSchema[2], random.nextInt(len) + 1, characterStrategy);
                        } else {
                            randomKey = RandomUtil.createRandomKey(random, valueSchema[3], valueSchema[2], characterStrategy);
                        }
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

}
