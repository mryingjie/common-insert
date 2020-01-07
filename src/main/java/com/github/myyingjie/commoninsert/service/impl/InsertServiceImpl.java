package com.github.myyingjie.commoninsert.service.impl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.fastjson.JSON;
import com.github.myyingjie.commoninsert.bean.*;
import com.github.myyingjie.commoninsert.config.SQLExecutorConfig;
import com.github.myyingjie.commoninsert.exception.BizException;
import com.github.myyingjie.commoninsert.service.InsertService;
import com.github.myyingjie.commoninsert.strategy.DataSourceType;
import com.github.myyingjie.commoninsert.strategy.FieldType;
import com.github.myyingjie.commoninsert.strategy.InsertRule;
import com.github.myyingjie.commoninsert.strategy.SqlFieldType;
import com.github.myyingjie.commoninsert.util.DatasourceFileUtil;
import com.github.myyingjie.commoninsert.util.JarUtil;
import com.github.myyingjie.commoninsert.util.JsonFormatTool;
import com.github.myyingjie.commoninsert.util.RandomUtil;
import com.heitaox.sql.executor.SQLExecutor;
import com.heitaox.sql.executor.core.entity.Tuple2;
import com.heitaox.sql.executor.core.util.DateUtils;
import com.heitaox.sql.executor.source.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * created by Yingjie Zheng at 2019-10-12 17:23
 */
@Service
@Slf4j
public class InsertServiceImpl implements InsertService, InitializingBean {

    @Autowired
    private SQLExecutor sqlExecutor;

    @Autowired
    private SQLExecutorConfig sqlExecutorConfig;


    private final Map<String, InsertParam> insertParamMap = new ConcurrentHashMap<>();

    private final static String RESOURCE_FILE_NAME = "history.json";


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
    public  int insert(InsertParam insertParam) throws Exception {
        String database = insertParam.getDatabase();
        if (!sqlExecutorConfig.dataSourcePropertiesMap.containsKey(insertParam.getDatabase())) {
            throw new BizException("no data source of " + insertParam.getDatabase() + " find,Please configure the data source first ");
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
        int i = 0;
        //插入数据
        try {
             i = sqlExecutor.executeInsert(sql);
        }catch (Throwable e){
            throw new BizException("数据插入失败，请检查数据源连接配置是否正确，或检查运行日志中生成的sql是否有误");
        }

        return i;
    }

    @Override
    public InsertParamVo detail(String tableName) {
        InsertParam insertParam = insertParamMap.get(tableName);
        if (insertParam == null) {
            throw new BizException("no tableName:{" + tableName + "} of param find");
        }
        InsertParamVo insertParamVo = new InsertParamVo();
        BeanUtils.copyProperties(insertParam, insertParamVo);
        return insertParamVo;
    }

    @Override
    public synchronized void delete(String tableName) throws IOException {
        String s = DatasourceFileUtil.readResourcesJsonFile(RESOURCE_FILE_NAME);
        List<InsertParam> list = JSON.parseArray(s, InsertParam.class);
        list.removeIf(next -> next.getTableName().equalsIgnoreCase(tableName));
        String json = "[\n" +
                "]";
        if (list.size() != 0) {
            json = JSON.toJSONString(list);
        }
        DatasourceFileUtil.write(RESOURCE_FILE_NAME, JsonFormatTool.formatJson(json));

        insertParamMap.remove(tableName);
    }

    @Override
    public List<ParamListVo> paramList() {
        return insertParamMap
                .values()
                .stream()
                .map(insertParam -> new ParamListVo(insertParam.getTableName(), insertParam.getDatabase()))
                .collect(Collectors.toList());
    }


    @Override
    public InsertParamVo transform(String sql) {
        List<SQLStatement> sqlStatements = SQLUtils.parseStatements(sql, "mysql");
        SQLStatement sqlStatement = sqlStatements.get(0);
        //查询语句
        InsertParamVo insertParamVo = new InsertParamVo();
        if (sqlStatement instanceof SQLCreateTableStatement) {
            SQLCreateTableStatement createStatement = (SQLCreateTableStatement) sqlStatement;
            SQLExprTableSource tableSource = createStatement.getTableSource();
            String tableName = ((SQLIdentifierExpr) tableSource.getExpr()).getName();

            insertParamVo.setTableName(removeFloat(tableName));
            List<SQLTableElement> tableElementList = createStatement.getTableElementList();
            LinkedHashMap<String, String> constant = tableElementList.stream()
                    .filter(sqlTableElement -> sqlTableElement instanceof SQLColumnDefinition)
                    .map(sqlTableElement -> (SQLColumnDefinition) sqlTableElement)
                    .map(sqlColumnDefinition -> {
                        SQLIdentifierExpr name = (SQLIdentifierExpr) sqlColumnDefinition.getName();
                        SQLDataType dataType = sqlColumnDefinition.getDataType();
                        FieldType fieldType = SqlFieldType.transToFieldType(dataType.getName());
                        String type = fieldType.getValue();
                        String value = dataType.toString();

                        return new Tuple2<>(removeFloat(name.getName()), type+"|"+value);
                    }).reduce(
                            new LinkedHashMap<>(),
                            (map, tuple2) -> {
                                map.put(tuple2.getV1(), tuple2.getV2());
                                return map;
                            },
                            (map1, map2) -> {
                                map1.putAll(map2);
                                return map1;
                            }
                    );
            insertParamVo.setConstant(constant);
        } else {
            throw new BizException("只能接收建表语句！！！");
        }
        insertParamMap.put(insertParamVo.getTableName(), insertParamVo);
        return insertParamVo;
    }

    @Override
    public void save(InsertParam insertParam) throws IOException {
        Objects.requireNonNull(insertParam.getTableName(), "表名不能为空！");
        Objects.requireNonNull(insertParam.getDatabase(), "库名不能为空！");
        insertParamMap.put(insertParam.getTableName(), insertParam);
        persistence(insertParam.getTableName());

    }

    private String removeFloat(String str) {
        if (str.startsWith("`")) {
            str = str.replaceAll("`", "");
        }
        return str;
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
                String[] valueSchema = entry.getValue().split(ConStant.SEPARATOR);
                InsertRule.CONSTANT.checkParam(valueSchema,field);
                fieldValueIndex.add(new Tuple2<>(valueSchema, InsertRule.CONSTANT));
            }
        }

        LinkedHashMap<String, String> randomField = insertParam.getRandom();
        if (randomField != null) {
            for (Map.Entry<String, String> entry : randomField.entrySet()) {
                String field = entry.getKey();
                sb.append(field).append(", ");
                String[] valueSchema = entry.getValue().split(ConStant.SEPARATOR);
                InsertRule.RANDOM.checkParam(valueSchema,field);
                fieldValueIndex.add(new Tuple2<>(valueSchema, InsertRule.RANDOM));
            }
        }

        LinkedHashMap<String, String> increaseField = insertParam.getIncrease();
        if (increaseField != null) {
            for (Map.Entry<String, String> entry : increaseField.entrySet()) {
                String field = entry.getKey();
                sb.append(field).append(", ");
                String[] valueSchema = entry.getValue().split(ConStant.SEPARATOR);
                InsertRule.INCREASE.checkParam(valueSchema,field);
                fieldValueIndex.add(new Tuple2<>(valueSchema, InsertRule.INCREASE));
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
                    throw new BizException("最大日期时间必须大于最小日期时间");
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
                    throw new BizException("最大日期必须大于最小日期");
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

    @Override
    public void afterPropertiesSet() throws Exception {
        //读取 history.json文件
        String s = DatasourceFileUtil.readResourcesJsonFile(RESOURCE_FILE_NAME);
        List<InsertParam> insertParams = JSON.parseArray(s, InsertParam.class);
        insertParams.forEach(insertParam -> insertParamMap.put(insertParam.getTableName(), insertParam));
        log.info("init insertParamList completed!");
    }

    @Override
    @SuppressWarnings("all")
    public void persistence(String tableName) throws IOException {
        //读取 history.json文件
        String s = DatasourceFileUtil.readResourcesJsonFile(RESOURCE_FILE_NAME);
        List<InsertParam> insertParams = JSON.parseArray(s, InsertParam.class);
        insertParams.removeIf(new Predicate<InsertParam>() {
            @Override
            public boolean test(InsertParam insertParam) {
                return insertParam.getTableName().equalsIgnoreCase(tableName);
            }
        });
        insertParams.add(insertParamMap.get(tableName));
        //写回文件
        String json = JsonFormatTool.formatJson(JSON.toJSONString(insertParams));
        String filePath = JarUtil.getJarDir()+File.separator + "data"+File.separator+RESOURCE_FILE_NAME;
        if(filePath.startsWith("file:")){
            filePath = filePath.substring(5);
        }
        File jsonFile = new File(filePath);

        try(FileOutputStream fis = new FileOutputStream(jsonFile)) {
            fis.write(json.getBytes(StandardCharsets.UTF_8));
        }

    }
}
