package com.github.myyingjie.commoninsert.util;

import com.github.myyingjie.commoninsert.bean.ConStant;

import java.math.BigDecimal;
import java.util.Random;

/**
 * created by Yingjie Zheng at 2019-09-27 15:11
 */
@SuppressWarnings("all")
public class RandomUtil {

    /**
     * 如果有最大最小值限制则必定返回一个数字
     * 如果没有则返回10位任意字符的字符串
     */
    public static String createRandomKey(Random random, String minVal, String maxVal, boolean isNum) {
        //长度无限制
        if (ConStant.ANY2.equals(minVal) && ConStant.ANY2.equals(maxVal)) {
            //任意字符
            return randomStr(random, 10, isNum);
        }

        BigDecimal max = new BigDecimal(maxVal);
        BigDecimal randomNum = new BigDecimal(0);
        BigDecimal min = new BigDecimal(minVal);
        int length = maxVal.length();
        StringBuilder sb = new StringBuilder();

        if (ConStant.ANY2.equals(minVal)) {
            //有最大值
            do {
                sb = new StringBuilder();
                for (int i = 0; i < length; i++) {
                    sb.append(random.nextInt(10));
                }
                randomNum = new BigDecimal(sb.toString());
            } while (randomNum.compareTo(max) > 0);
            return sb.toString();
        } else if (ConStant.ANY2.equals(maxVal)) {
            //有最小值
            do {
                sb = new StringBuilder();
                for (int i = 0; ; i++) {
                    if (i >= minVal.length() - 1) {
                        if (random.nextInt(2) == 1) {
                            break;
                        }
                    }
                    sb.append(random.nextInt(10));
                }
                randomNum = new BigDecimal(sb.toString());
            } while (randomNum.compareTo(min) < 0);
            return sb.toString();
        } else {
            // 既有最大值 也有最小值
            //有最小值
            if (max.compareTo(min) < 0) {
                throw new RuntimeException("最大值不能小于最小值");
            }
            if (min.compareTo(new BigDecimal(0)) < 0) {
                throw new RuntimeException("不能生成负数");
            }
            do {
                sb = new StringBuilder();
                for (int i = 0; i < maxVal.length(); i++) {
                    if (i >= minVal.length() - 1) {
                        if (random.nextInt(2) == 1) {
                            break;
                        }
                    }
                    sb.append(random.nextInt(10));
                }
                randomNum = new BigDecimal(sb.toString());
            } while (!(randomNum.compareTo(min) >= 0 && randomNum.compareTo(max) <= 0));

            return sb.toString();
        }
    }


    public static String createRandomKey(Random random, String minVal, String maxVal, int len, boolean isNum) {
        //长度无限制
        if (ConStant.ANY2.equals(minVal) && ConStant.ANY2.equals(maxVal)) {
            //任意字符
            return randomStr(random, len, isNum);
        }
        String randomKey = createRandomKey(random, minVal, maxVal, isNum);
        return appendHeadZero(randomKey, len);
    }

    public static void main(String[] args) {
        System.out.println(appendHeadZero("12232", 3));
        for (int i = 0; i < 100; i++) {

            System.out.println(new Random().nextDouble());
        }
    }


    public static String appendHeadZero(String str, int len) {
        int length = str.length();
        int tmplen = len - length;
        if (tmplen < 0) {
            len = length - len;
            return str.substring(0, length - len);
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tmplen; i++) {
            sb.append(0);
        }
        sb.append(str);
        return sb.toString();
    }

    public static String appendTailZero(String str, int len) {
        int length = str.length();
        len = len - length;
        if (len < 0) {
            len = length - len;
            return str.substring(0, length - len);
        }
        StringBuilder sb = new StringBuilder(str);
        for (int i = 0; i < len; i++) {
            sb.append(0);
        }
        return sb.toString();
    }


    /**
     * 随机生成任意长度的字符
     */
    public static String randomStr(Random random, int len, boolean isNum) {
        if (len == -1) {
            len = 10;
        }
        StringBuilder sb = new StringBuilder();
        if (!isNum) {
            int i = len;//控制字符长度
            for (int j = 0; j < i; j++) {
                //生成一个97-122之间的int类型整数--为了生成小写字母
                int intValL = (int) (random.nextDouble() * 26 + 97);
                //生成一个65-90之间的int类型整数--为了生成大写字母
                int intValU = (int) (random.nextDouble() * 26 + 65);
                //生成一个30-39之间的int类型整数--为了生成数字
                int intValN = (int) (random.nextDouble() * 10 + 48);

                int intVal = 0;
                int r = (int) (random.nextDouble() * 3);

                if (r == 0) {
                    intVal = intValL;
                } else if (r == 1) {
                    intVal = intValU;
                } else {
                    intVal = intValN;
                }

                sb.append((char) intVal);
            }
            return sb.toString();
        } else {
            for (int i = 0; i < len; i++) {
                sb.append(random.nextInt(10));
            }
            return sb.toString();
        }


    }


}
