package com.github.myyingjie.commoninsert.controller;


import com.github.myyingjie.commoninsert.bean.AjaxResult;

/**
 * @Author ZhengYingjie
 * @Date 2018/11/25
 * @Description
 * @see AjaxResult
 */
public abstract class AjaxBaseController {

    private ThreadLocal<AjaxResult> threadLocal = null;

    public void start() {
        if (threadLocal == null) {
            threadLocal = new ThreadLocal<>();
        }
        threadLocal.set(new AjaxResult());
    }

    public void success(boolean flag) {
        AjaxResult ajaxResult = threadLocal.get();
        ajaxResult.setSuccess(flag);
    }

    public void success() {
        success(true);
    }

    public void fail() {
        AjaxResult ajaxResult = threadLocal.get();
        ajaxResult.setSuccess(false);
    }

    public void set(Object obj) {
        AjaxResult ajaxResult = threadLocal.get();
        ajaxResult.setData(obj);
    }

    public Object get() {
        AjaxResult ajaxResult = threadLocal.get();
        return ajaxResult.getData();
    }

    public AjaxResult end() {
        AjaxResult ajaxResult = threadLocal.get();
        threadLocal.remove();
        return ajaxResult;
    }
}
