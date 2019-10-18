package com.github.myyingjie.commoninsert.strategy;

import com.github.myyingjie.commoninsert.bean.ConStant;
import com.github.myyingjie.commoninsert.exception.BizException;
import com.heitaox.sql.executor.core.util.DateUtils;
import org.springframework.util.StringUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * created by Yingjie Zheng at 2019-09-27 14:03
 */
public enum InsertRule {
    /**
     * 随机
     */
    RANDOM {
        @Override
        public boolean checkParam(String[] valueSchema, String field) {
            String type = valueSchema[0];
            if (FieldType.getByValue(type) == null) {
                throw new BizException("随机字段[" + field + "]字符类型不合法");
            }
            FieldType fieldType = FieldType.getByValue(type);
            String max = valueSchema[2];
            String min = valueSchema[3];
            if (fieldType.equals(FieldType.DATE)) {
                LocalDate maxDate = null;
                try {
                    if (!max.equalsIgnoreCase(ConStant.ANY2)) {
                        maxDate = LocalDate.parse(max, DateUtils.yyyyMMdd);
                    }
                } catch (RuntimeException ex) {
                    throw new BizException("随机字段[" + field + "]最大值必须是yyyy-MM-dd格式的日期");
                }
                LocalDate minDate = null;
                try {
                    if (!min.equalsIgnoreCase(ConStant.ANY2)) {
                        minDate = LocalDate.parse(min, DateUtils.yyyyMMdd);
                    }
                } catch (RuntimeException ex) {
                    throw new BizException("随机字段[" + field + "]最小值必须是yyyy-MM-dd格式的日期");
                }

                if(minDate != null && maxDate != null){
                    if (maxDate.isBefore(minDate)) {
                        throw new BizException("随机字段[" + field + "]最小值必须小于最大值");
                    }
                }


            } else if (fieldType.equals(FieldType.DATETIME)) {

                LocalDateTime maxDate = null;
                try {
                    if (!max.equalsIgnoreCase(ConStant.ANY2)) {
                        maxDate = LocalDateTime.parse(max, DateUtils.dateTimeFormatter);
                    }
                } catch (RuntimeException ex) {
                    throw new BizException("随机字段[" + field + "]最大值必须是yyyy-MM-dd HH:mm:ss格式的日期时间");
                }
                LocalDateTime minDate = null;
                try {
                    if (!min.equalsIgnoreCase(ConStant.ANY2)) {
                        minDate = LocalDateTime.parse(min, DateUtils.dateTimeFormatter);
                    }
                } catch (RuntimeException ex) {
                    throw new BizException("随机字段[" + field + "]最小值必须是yyyy-MM-dd HH:mm:ss格式的日期时间");
                }
                if(minDate != null && maxDate != null){
                    if (maxDate.isBefore(minDate)) {
                        throw new BizException("随机字段[" + field + "]最小值必须小于最大值");
                    }
                }
            } else {
                String length = valueSchema[1];
                try {
                    int i = Integer.parseInt(length);
                    if (i <= 0 && i != -1) {
                        throw new NumberFormatException();
                    }
                } catch (NumberFormatException ex) {
                    throw new BizException("随机字段[" + field + "]长度必须为大于0的整数");
                }

                if (fieldType.equals(FieldType.INTEGER)) {
                    Integer maxData = null;
                    try {
                        maxData = Integer.parseInt(max);
                        if (maxData < 0 && maxData != -1) {
                            throw new NumberFormatException();
                        }
                    } catch (NumberFormatException ex) {
                        throw new BizException("随机字段[" + field + "]最大值必须为大于0的整数");
                    }
                    Integer minData = null;
                    try {
                        minData = Integer.parseInt(min);
                        if (minData < 0 && minData != -1) {
                            throw new NumberFormatException();
                        }
                    } catch (NumberFormatException ex) {
                        throw new BizException("随机字段[" + field + "]最小值必须为大于0的整数");
                    }
                    if(minData != -1 && maxData != -1){
                        if (maxData <= minData) {
                            throw new BizException("随机字段[" + field + "]最小值小于最大值");
                        }
                    }

                } else if (fieldType.equals(FieldType.LONG)) {
                    Long maxData = null;
                    try {
                        maxData = Long.parseLong(max);
                        if (maxData < 0 && maxData != -1) {
                            throw new NumberFormatException();
                        }
                    } catch (NumberFormatException ex) {
                        throw new BizException("随机字段[" + field + "]最大值必须为大于0的整数");
                    }
                    Long minData = null;
                    try {
                        minData = Long.parseLong(min);
                        if (minData < 0 && minData != -1) {
                            throw new NumberFormatException();
                        }
                    } catch (NumberFormatException ex) {
                        throw new BizException("随机字段[" + field + "]最小值必须为大于0的整数");
                    }
                    if(minData != -1 && maxData != -1){
                        if (maxData <= minData) {
                            throw new BizException("随机字段[" + field + "]最小值小于最大值");
                        }
                    }
                } else if (fieldType.equals(FieldType.DOUBLE) || fieldType.equals(FieldType.DECIMAL)) {
                    Double maxData = null;
                    try {
                        maxData = Double.parseDouble(max);
                        if (maxData < 0 && maxData != -1) {
                            throw new NumberFormatException();
                        }
                    } catch (NumberFormatException ex) {
                        throw new BizException("随机字段[" + field + "]最大值必须为大于0的整数或小数");
                    }
                    Double minData = null;
                    try {
                        minData = Double.parseDouble(min);
                        if (minData < 0 && minData != -1) {
                            throw new NumberFormatException();
                        }
                    } catch (NumberFormatException ex) {
                        throw new BizException("随机字段[" + field + "]最小值必须为大于0的整数或小数");
                    }
                    if(minData != -1 && maxData != -1){
                        if (maxData <= minData) {
                            throw new BizException("随机字段[" + field + "]最小值小于最大值");
                        }
                    }
                    String decimalLength = valueSchema[9];
                    try {
                        int i = Integer.parseInt(decimalLength);
                        if (i <= 0 && i != -1) {
                            throw new NumberFormatException();
                        }
                    } catch (NumberFormatException ex) {
                        throw new BizException("随机字段[" + field + "]小数位数必须为大于0的整数");
                    }
                }
                String isFixlength = valueSchema[4];
                if (!(isFixlength.equalsIgnoreCase("true") || isFixlength.equalsIgnoreCase("false"))) {
                    throw new BizException("随机字段[" + field + "]是否固定位数必须是布尔值true或false");
                }
                String charStrategy = valueSchema[8];
                CharacterStrategy byStrategyCode = null;
                try {
                    byStrategyCode = CharacterStrategyEnum.getByStrategyCode(Integer.parseInt(charStrategy));
                } catch (RuntimeException ex) {
                    throw new BizException("随机字段[" + field + "]字符策略不合法");
                }
                if (byStrategyCode == null) {
                    throw new BizException("随机字段[" + field + "]不存在的字符策略");
                }

            }
            String inUnique = valueSchema[7];
            if (!(inUnique.equalsIgnoreCase("true") || inUnique.equalsIgnoreCase("false"))) {
                throw new BizException("随机字段[" + field + "]是否唯一必须是布尔值true或false");
            }
            return true;
        }
    },
    /**
     * 常量
     */
    CONSTANT {
        @Override
        public boolean checkParam(String[] valueSchema, String field) {
            if (valueSchema.length != 2) {
                throw new BizException("常量字段[" + field + "]生成规则不完整");
            }
            String type = valueSchema[0];
            if (FieldType.getByValue(type) == null) {
                throw new BizException("常量字段[" + field + "]字符类型不合法");
            }
            FieldType fieldType = FieldType.getByValue(type);
            String value = valueSchema[1];
            if (StringUtils.isEmpty(value)) {
                throw new BizException("常量字段[" + field + "]值为空");
            }
            String[] split = value.split(",");
            if (fieldType.equals(FieldType.DATE)) {
                try {
                    for (String s : split) {
                        LocalDate.parse(s, DateUtils.yyyyMMdd);
                    }
                } catch (RuntimeException ex) {
                    throw new BizException("常量字段[" + field + "]值必须都是yyyy-MM-dd格式的日期");
                }
            } else if (fieldType.equals(FieldType.DATETIME)) {
                try {
                    for (String s : split) {
                        LocalDateTime.parse(s, DateUtils.dateTimeFormatter);
                    }
                } catch (RuntimeException ex) {
                    throw new BizException("常量字段[" + field + "]值必须都是yyyy-MM-dd HH:mm:ss格式的日期时间");
                }
            } else if (fieldType.equals(FieldType.INTEGER)) {
                try {
                    for (String s : split) {
                        Integer.parseInt(s);
                    }
                } catch (NumberFormatException ex) {
                    throw new BizException("常量字段[" + field + "]值必须都是整数类型");
                }
            } else if (fieldType.equals(FieldType.LONG)) {
                try {
                    for (String s : split) {
                        Long.parseLong(s);
                    }
                } catch (NumberFormatException ex) {
                    throw new BizException("常量字段[" + field + "]值必须都是整数类型");
                }
            } else if (fieldType.equals(FieldType.DECIMAL) || fieldType.equals(FieldType.DOUBLE)) {
                try {
                    for (String s : split) {
                        Double.parseDouble(s);
                    }
                } catch (NumberFormatException ex) {
                    throw new BizException("常量字段[" + field + "]值必须都是数字类型");
                }
            }
            return true;
        }
    },
    /**
     * 自增
     */
    INCREASE {
        @Override
        public boolean checkParam(String[] valueSchema, String field) {
            String type = valueSchema[0];
            if (FieldType.getByValue(type) == null) {
                throw new BizException("自增字段[" + field + "]字符类型不合法");
            }
            FieldType fieldType = FieldType.getByValue(type);
            String from = valueSchema[1];

            if (fieldType.equals(FieldType.DATE)) {
                try {
                    LocalDate.parse(from, DateUtils.yyyyMMdd);
                } catch (RuntimeException ex) {
                    throw new BizException("自增字段[" + field + "]从几开始必须是yyyy-MM-dd格式的日期");
                }
            } else if (fieldType.equals(FieldType.DATETIME)) {
                try {
                    LocalDateTime.parse(from, DateUtils.dateTimeFormatter);
                } catch (RuntimeException ex) {
                    throw new BizException("自增字段[" + field + "]从几开始必须是yyyy-MM-dd HH:mm:ss格式的日期时间");
                }
            } else if (fieldType.equals(FieldType.INTEGER)) {
                try {
                    Integer.parseInt(from);
                } catch (NumberFormatException ex) {
                    throw new BizException("自增字段[" + field + "]从几开始必须都是整数类型");
                }
            } else if (fieldType.equals(FieldType.LONG)) {
                try {
                    Long.parseLong(from);
                } catch (NumberFormatException ex) {
                    throw new BizException("自增字段[" + field + "]从几开始必须都是整数类型");
                }
            } else if (fieldType.equals(FieldType.DECIMAL) || fieldType.equals(FieldType.DOUBLE) || fieldType.equals(FieldType.STRING)) {
                try {
                    Double.parseDouble(from);
                } catch (NumberFormatException ex) {
                    throw new BizException("自增字段[" + field + "]值必须都是数字或时间类型");
                }
            }
            String length = valueSchema[2];
            try {
                int i = Integer.parseInt(length);
                if (i <= 0 && i != -1) {
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException ex) {
                throw new BizException("自增字段[" + field + "]位数必须为大于0的整数");
            }
            return true;
        }
    };

    public abstract boolean checkParam(String[] valueSchema, String field);
}
