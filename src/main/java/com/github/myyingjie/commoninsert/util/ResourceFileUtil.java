package com.github.myyingjie.commoninsert.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ResourceUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * created by Yingjie Zheng at 2019-10-09 16:10
 */
@Slf4j
public class ResourceFileUtil {
    public static String readResourcesJsonFile(String fileName) throws IOException {
        File jsonFile = ResourceUtils.getFile("classpath:" + fileName);
        try (InputStreamReader reader = new InputStreamReader(new FileInputStream(jsonFile))) {
            int ch;
            StringBuilder sb = new StringBuilder();
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            return sb.toString();

        }
    }

    public static void write(String fileName, String data) throws IOException {
        File jsonFile = ResourceUtils.getFile("classpath:" + fileName);
        try(FileOutputStream fis = new FileOutputStream(jsonFile)) {
            fis.write(data.getBytes(StandardCharsets.UTF_8));
        }
    }
}
