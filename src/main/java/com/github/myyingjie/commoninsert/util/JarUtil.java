package com.github.myyingjie.commoninsert.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileNotFoundException;

@Slf4j
public class JarUtil {

    /**
     * 获取jar绝对路径
     *
     * @return
     */
    public static String getJarPath() {
        File file = getClassFile();
        if (file == null) {
            return null;
        }
        return file.getAbsolutePath();
    }

    /**
     * 获取jar目录
     * return 当前jar包所在路径
     */
    public static String getJarDir() {
        try {
            return new File(ResourceUtils.getURL("classpath:").getPath()).getParentFile().getParentFile().getParent();
        } catch (FileNotFoundException e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }


    /**
     * 获取当前Jar文件
     *
     * @return classes 文件所在路径
     */
    private static File getClassFile() {
        // 关键是这行...
        String path = JarUtil.class.getProtectionDomain().getCodeSource().getLocation().getFile();
        try {
            path = java.net.URLDecoder.decode(path, "UTF-8");
        } catch (java.io.UnsupportedEncodingException e) {
            return null;
        }
        return new File(path);
    }

}
