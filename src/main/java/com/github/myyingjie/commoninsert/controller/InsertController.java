package com.github.myyingjie.commoninsert.controller;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.github.myyingjie.commoninsert.bean.AjaxResult;
import com.github.myyingjie.commoninsert.bean.DataSourceProperties;
import com.github.myyingjie.commoninsert.bean.DataSourcePropertiesVo;
import com.github.myyingjie.commoninsert.bean.InsertParam;
import com.github.myyingjie.commoninsert.service.InsertService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;


/**
 * created by Yingjie Zheng at 2019-09-27 10:33
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
        log.info("datasource: {} ,InsertParam:{}", insertParam.getType(),JSON.toJSONString(insertParam));
        int insert = 0;
        try {
            insert = insertService.insert(insertParam);
            success();
            set(insert);
        } catch (IOException | SQLException e) {
            log.error(e.getMessage(),e);
            set(e.getMessage());
            fail();
        }
        return end();
    }

    /**
     * 查询数据源列表
     * @param status 查询条件 0不查 1数据库 2文件 3不查
     */
    @ResponseBody
    @RequestMapping(value = "/datasource/{status}",method = RequestMethod.GET)
    public AjaxResult queryDatasource(@PathVariable("status") int status){
        start();
        log.info("query datasource,status:{}", status);
        try {
            List<DataSourcePropertiesVo> dataSources = insertService.queryDatasource(status);
            success();
            set(dataSources);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            set(e.getMessage());
            fail();
        }
        return end();
    }

    /**
     * 更新数据源
     */
    @ResponseBody
    @RequestMapping(value = "/datasource",method = RequestMethod.PUT)
    public AjaxResult updateDatasource(@RequestBody DataSourceProperties dataSourceProperties){
        start();
        log.info("update datasource,dataSourceProperties:{}", JSON.toJSONString(dataSourceProperties));
        try {
            insertService.updateDatasource(dataSourceProperties);
            success();
        }catch (Exception e){
            log.error(e.getMessage(),e);
            set(e.getMessage());
            fail();
        }
        return end();
    }

    /**
     * 删除数据源
     */
    @ResponseBody
    @RequestMapping(value = "/datasource/{database}",method = RequestMethod.DELETE)
    public AjaxResult deleteDatasource(@PathVariable("database") String database){
        start();
        log.info("delete datasource,database:{}", database);
        try {
            insertService.deleteDatasource(database);
            success();
        }catch (Exception e){
            log.error(e.getMessage(),e);
            set(e.getMessage());
            fail();
        }
        return end();
    }

    /**
     * 添加数据源
     */
    @ResponseBody
    @RequestMapping(value = "/datasource",method = RequestMethod.POST)
    public AjaxResult addDatasource(@RequestBody DataSourceProperties dataSourceProperties){
        start();
        dataSourceProperties.setPersistenced(false);
        log.info("add datasource : {}", JSON.toJSONString(dataSourceProperties));
        try {

            insertService.addDatasource(dataSourceProperties);
            success();
        }catch (Exception e){
            log.error(e.getMessage(),e);
            set(e.getMessage());
            fail();
        }
        return end();
    }

    /**
     * 持久化
     */
    @ResponseBody
    @RequestMapping(value = "/datasource/{database}",method = RequestMethod.PUT)
    public AjaxResult persistence(@PathVariable("database") String database){
        start();
        log.info("持久化数据源 database:{}", database);
        try {
            insertService.persistence(database);
            success();
        }catch (Exception e){
            log.error(e.getMessage(), e);
            set(e.getMessage());
            fail();
        }
        return end();
    }



    @RequestMapping("index")
    public String index(){
        return "index";
    }

    @RequestMapping("datasource")
    public String datasource(){
        return "datasource";
    }

    @RequestMapping("main")
    public String main(){
        return "main";
    }



}
