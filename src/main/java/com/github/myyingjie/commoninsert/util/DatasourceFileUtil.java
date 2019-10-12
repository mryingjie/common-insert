package com.github.myyingjie.commoninsert.util;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * created by Yingjie Zheng at 2019-10-09 16:10
 */
@Slf4j
public class DatasourceFileUtil {
    public static String readResourcesJsonFile(String fileName) throws IOException {
        // ClassPathResource resource = new ClassPathResource(fileName);
        String filePath = JarUtil.getJarDir()+File.separator + "data"+File.separator+fileName;
        if(filePath.startsWith("file:")){
            filePath = filePath.substring(5);
        }
        try (InputStreamReader reader = new InputStreamReader(new FileInputStream(filePath))) {
            int ch;
            StringBuilder sb = new StringBuilder();
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            return sb.toString();

        }
    }

    public static void write(String fileName, String data) throws IOException {
        String filePath = JarUtil.getJarDir()+File.separator + "data"+File.separator+fileName;
        if(filePath.startsWith("file:")){
            filePath = filePath.substring(5);
        }
        File jsonFile = new File(filePath);

        try(FileOutputStream fis = new FileOutputStream(jsonFile)) {
            fis.write(data.getBytes(StandardCharsets.UTF_8));
        }
    }

}
