package com.github.myyingjie.commoninsert.controller;
import java.util.List;
import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.github.myyingjie.commoninsert.bean.AjaxResult;
import com.github.myyingjie.commoninsert.bean.DataSourceProperties;
import com.github.myyingjie.commoninsert.bean.DataSourcePropertiesVo;
import com.github.myyingjie.commoninsert.exception.BizException;
import com.github.myyingjie.commoninsert.service.DatsourceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;


/**
 * created by Yingjie Zheng at 2019-09-27 10:33
 */
@Controller
@Slf4j
public class DatasourceController extends AjaxBaseController{

    @Autowired
    private DatsourceService datsourceService;



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
            List<DataSourcePropertiesVo> dataSources = datsourceService.queryDatasource(status);
            success();
            set(dataSources);
        }catch (BizException e){
            log.error(e.getMessage(),e);
            set(e.getMessage());
            fail();
        }catch (Throwable e){
            log.error("系统错误",e);
            set("系统错误");
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
            datsourceService.updateDatasource(dataSourceProperties);
            success();
        }catch (BizException e){
            log.error(e.getMessage(),e);
            set(e.getMessage());
            fail();
        }catch (Throwable e){
            log.error("系统错误",e);
            set("系统错误");
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
            datsourceService.deleteDatasource(database);
            success();
        }catch (BizException e){
            log.error(e.getMessage(),e);
            set(e.getMessage());
            fail();
        }catch (Throwable e){
            log.error("系统错误",e);
            set("系统错误");
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

            datsourceService.addDatasource(dataSourceProperties);
            success();
        }catch (BizException e){
            log.error(e.getMessage(),e);
            set(e.getMessage());
            fail();
        }catch (Throwable e){
            log.error("系统错误",e);
            set("系统错误");
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
            datsourceService.persistence(database);
            success();
        }catch (BizException e){
            log.error(e.getMessage(), e);
            set(e.getMessage());
            fail();
        }catch (Throwable e){
            log.error("系统错误",e);
            set("系统错误");
            fail();
        }
        return end();
    }

    @ResponseBody
    @RequestMapping(value = "database/list",method = RequestMethod.GET)
    public AjaxResult databaseList(){
        start();
        log.info("query databaseList");
        try {
            Set<String> databaseList = datsourceService.queryDatabase();
            success();
            set(databaseList);
        }catch (BizException e){
            log.error(e.getMessage(), e);
            set(e.getMessage());
            fail();
        }catch (Throwable e){
            log.error("系统错误",e);
            set("系统错误");
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





}
