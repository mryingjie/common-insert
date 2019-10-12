package com.github.myyingjie.commoninsert.strategy;

import com.github.myyingjie.commoninsert.util.RandomUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * created by Yingjie Zheng at 2019-10-09 17:39
 */
public enum CharacterStrategyEnum implements CharacterStrategy {
    // 0:任意 1:纯数字 2:纯字母 3:纯汉字 4:数字+字母 5:数字+汉字 6:字母+汉字


    /**
     * 任意
     */
    ANY(0) {
        @Override
        public String randomStr(Random random, int len) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < len; j++) {
                int i = random.nextInt(3);
                if (i == 0) {
                    // 生成数字
                    sb.append(RandomUtil.randomNumber(random));
                } else if (i == 1) {
                    //生成字母
                    sb.append(RandomUtil.randomAlphabet(random));
                } else {
                    sb.append(RandomUtil.randomHan(random));
                }
            }
            return sb.toString();
        }
    },
    /**
     * 纯数字
     */
    NUMBER(1) {
        @Override
        public String randomStr(Random random, int len) {
            return RandomUtil.randomNumber(random, len);
        }
    },
    /**
     * 纯字母
     */
    ALPHABET(2) {
        @Override
        public String randomStr(Random random, int len) {
            return RandomUtil.randomAlphabet(random, len);
        }
    },
    /**
     * 纯汉字
     */
    CHINESE(3) {
        @Override
        public String randomStr(Random random, int len) {
            return RandomUtil.randomHan(random, len);
        }
    },
    /**
     * 数字和字母的组合
     */
    NUMBER_ALPHABET(4) {
        @Override
        public String randomStr(Random random, int len) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < len; j++) {
                int i = random.nextInt(2);
                if (i == 0) {
                    // 生成数字
                    sb.append(RandomUtil.randomNumber(random));
                } else {
                    //生成字母
                    sb.append(RandomUtil.randomAlphabet(random));
                }
            }
            return sb.toString();
        }
    },
    /**
     * 数字和汉字
     */
    NUMBER_CHINESE(5){
        @Override
        public String randomStr(Random random, int len) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < len; j++) {
                int i = random.nextInt(2);
                if (i == 0) {
                    // 生成数字
                    sb.append(RandomUtil.randomNumber(random));
                } else {
                    //生成汉字
                    sb.append(RandomUtil.randomHan(random));
                }
            }
            return sb.toString();
        }
    },
    /**
     * 字母和汉字
     */
    ALPHABET_CHINESE(6){
        @Override
        public String randomStr(Random random, int len) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < len; j++) {
                int i = random.nextInt(2);
                if (i == 0) {
                    // 生成字母
                    sb.append(RandomUtil.randomAlphabet(random));
                } else {
                    //生成汉字
                    sb.append(RandomUtil.randomHan(random));
                }
            }
            return sb.toString();
        }
    };

    private int value;

    private static final Map<Integer,CharacterStrategy> characterStrategyMap;


    CharacterStrategyEnum(int value) {
        this.value = value;
    }

    static {
        characterStrategyMap = new HashMap<>();
        for (CharacterStrategyEnum value : CharacterStrategyEnum.values()) {
            characterStrategyMap.put(value.value, value);
        }
    }

    public static CharacterStrategy getByStrategyCode(int code){
        return characterStrategyMap.get(code);
    }

    /**
     * 增加一个策略
     * @param code 策略编号
     * @param strategy 策略的实现
     */
    public  static void appendCharacterStrategy(int code,CharacterStrategy strategy){
        characterStrategyMap.put(code, strategy);
    }

}
