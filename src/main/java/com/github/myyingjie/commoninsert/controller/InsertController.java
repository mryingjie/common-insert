package com.github.myyingjie.commoninsert.controller;
import	java.security.BasicPermission;

import com.alibaba.fastjson.JSON;
import com.github.myyingjie.commoninsert.bean.AjaxResult;
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
 * created by Yingjie Zheng at 2019-10-12 17:21
 */
@Controller
@Slf4j
public class InsertController extends AjaxBaseController{

    @Autowired
    private InsertService insertService;


    /**
     * 生成并向对应的数据源插入数据
     */
    @ResponseBody
    @RequestMapping(value = "insert",method = RequestMethod.POST)
    public AjaxResult insert(@RequestBody InsertParam insertParam){
        start();
        log.info("InsertParam:{}", JSON.toJSONString(insertParam));
        int insert = 0;
        try {
            insert = insertService.insert(insertParam);
            success();
            set(insert);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
            set(e.getMessage());
            fail();
        }
        return end();
    }




    @RequestMapping("main")
    public String main(){
        return "main";
    }


}
