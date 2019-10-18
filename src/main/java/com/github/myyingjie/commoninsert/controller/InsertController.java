package com.github.myyingjie.commoninsert.controller;

import java.security.BasicPermission;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.github.myyingjie.commoninsert.bean.AjaxResult;
import com.github.myyingjie.commoninsert.bean.InsertParam;
import com.github.myyingjie.commoninsert.bean.InsertParamVo;
import com.github.myyingjie.commoninsert.bean.ParamListVo;
import com.github.myyingjie.commoninsert.exception.BizException;
import com.github.myyingjie.commoninsert.service.InsertService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

/**
 * created by Yingjie Zheng at 2019-10-12 17:21
 */
@Controller
@Slf4j
public class InsertController extends AjaxBaseController {

    @Autowired
    private InsertService insertService;




    /**
     * 获取详情
     */
    @ResponseBody
    @RequestMapping(value = "/history/{tableName}", method = RequestMethod.GET)
    public AjaxResult detail(@PathVariable("tableName") String tableName) {
        start();
        log.info("query detail tableName:{}", tableName);
        try {
            InsertParamVo insertParam;
            if (tableName.equalsIgnoreCase("default")) {
                insertParam = new InsertParamVo();
                insertParam.setTableName("");
                insertParam.setDatabase("");
            } else {

                insertParam = insertService.detail(tableName);
            }
            success();
            set(insertParam);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            fail();
            set(e.getMessage());
        }catch (Throwable e){
            log.error("系统错误",e);
            set("系统错误");
            fail();
        }
        return end();
    }

    /**
     * 删除
     */
    @ResponseBody
    @RequestMapping(value = "/history/{tableName}", method = RequestMethod.DELETE)
    public AjaxResult delete(@PathVariable("tableName") String tableName) {
        start();
        log.info("delete detail tableName:{}", tableName);
        try {
            insertService.delete(tableName);
            success();
        } catch (BizException e) {
            log.error(e.getMessage(), e);
            fail();
            set(e.getMessage());
        }catch (Throwable e){
            log.error("系统错误",e);
            set("系统错误");
            fail();
        }
        return end();
    }

    /**
     * 获取列表
     */
    @ResponseBody
    @RequestMapping(value = "/history/list", method = RequestMethod.GET)
    public AjaxResult paramList() {
        start();
        log.info("query history insertParam list");
        try {
            List<ParamListVo> paramList = insertService.paramList();
            success();
            set(paramList);
        } catch (BizException e) {
            log.error(e.getMessage(), e);
            fail();
            set(e.getMessage());
        }catch (Throwable e){
            log.error("系统错误",e);
            set("系统错误");
            fail();
        }
        return end();
    }

    /**
     * 转换sql为insertParam
     * @param sql
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/transform/sql", method = RequestMethod.POST)
    public AjaxResult transform(@RequestParam("sql") String sql) {
        start();
        log.info("transform sql:{}", sql);
        try {
            InsertParamVo insertParam = insertService.transform(sql);
            success();
            set(insertParam.getTableName());
        } catch (BizException e) {
            log.error(e.getMessage(), e);
            fail();
            set(e.getMessage());
        }catch (Throwable e){
            log.error("系统错误",e);
            set("系统错误");
            fail();
        }
        return end();
    }

    @ResponseBody
    @RequestMapping(value = "/history",method = RequestMethod.POST)
    public AjaxResult saveInsertParam(@RequestBody InsertParam insertParam){
        start();
        log.info("add insertParam : {}", JSON.toJSONString(insertParam));
        try {
            insertService.save(insertParam);
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
     * 生成并向对应的数据源插入数据
     */
    @ResponseBody
    @RequestMapping(value = "/insert",method = RequestMethod.POST)
    public AjaxResult saveAndInsert(@RequestBody InsertParam insertParam){
        start();
        log.info("execute insertParam : {}", JSON.toJSONString(insertParam));
        try {
            insertService.insert(insertParam);
            log.info("插入成功,保存本次提交参数");
            insertService.save(insertParam);
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


    @RequestMapping("main")
    public String main() {
        return "main";
    }

    @RequestMapping(value = "/insertPage/{tableName}", method = RequestMethod.GET)
    public ModelAndView insertPage(@PathVariable(value = "tableName", required = false) String tableName) {
        ModelAndView modelAndView = new ModelAndView();
        if (tableName != null) {
            modelAndView.addObject("tableName", tableName);
        }
        modelAndView.setViewName("insertParam");
        return modelAndView;
    }

    @RequestMapping(value = "/addPage/", method = RequestMethod.GET)
    public ModelAndView insertPage() {
        ModelAndView modelAndView = new ModelAndView();
        modelAndView.setViewName("insertParam");
        return modelAndView;
    }

}
