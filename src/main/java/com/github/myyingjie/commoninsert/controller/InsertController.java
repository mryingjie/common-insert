package com.github.myyingjie.commoninsert.controller;
import java.io.IOException;
import java.sql.SQLException;

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
    @RequestMapping(value = "mysql",method = RequestMethod.POST)
    public Object mysql(@RequestBody InsertParam insertParam){
        log.info("datasource: {} ,InsertParam:{}", "mysql",JSON.toJSONString(insertParam));
        int insert = 0;
        try {
            insert = insertService.insert(insertParam);
        } catch (IOException | SQLException e) {
            log.error(e.getMessage(),e);
        }

        return insert;
    }

    @ResponseBody
    @RequestMapping(value = "es",method = RequestMethod.POST)
    public Object es(@RequestBody InsertParam insertParam){
        log.info("datasource: {},InsertParam:{}","es", JSON.toJSONString(insertParam));
        int insert = 0;
        try {
            insert = insertService.insert(insertParam);
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }

        return insert;
    }


    @ResponseBody
    @RequestMapping(value = "mongo",method = RequestMethod.POST)
    public Object mongo(@RequestBody InsertParam insertParam){
        log.info("datasource: {},InsertParam:{}","mongo", JSON.toJSONString(insertParam));
        int insert = 0;
        try {
            insert = insertService.insert(insertParam);
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }

        return insert;
    }



}
