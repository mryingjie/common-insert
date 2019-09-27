package com.github.myyingjie.commoninsert.controller;
import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedHashMap;

import com.alibaba.fastjson.JSON;
import com.github.myyingjie.commoninsert.bean.InsertParam;
import com.github.myyingjie.commoninsert.service.InsertService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;


/**
 * created by Yingjie Zheng at 2019-09-27 10:33
 */
@Controller
@Slf4j
public class InsertController {

    @Autowired
    private InsertService insertService;

    @ResponseBody
    @RequestMapping(value = "insert",method = RequestMethod.POST)
    public Object insert(@RequestBody InsertParam insertParam){
        log.info("InsertParam:{}", JSON.toJSONString(insertParam));
        int insert = 0;
        try {
            insert = insertService.insert(insertParam);
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }

        return insert;
    }

}
