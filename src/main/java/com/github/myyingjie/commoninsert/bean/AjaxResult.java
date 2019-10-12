package com.github.myyingjie.commoninsert.bean;


import com.github.myyingjie.commoninsert.controller.AjaxBaseController;

import java.io.Serializable;

/**
 * @Author ZhengYingjie
 * @Date 2018/11/21
 * @Description
 * @see AjaxBaseController
 */
public class AjaxResult implements Serializable {

    /**
     * 方法执行的结果
     */
    private boolean success = false;

    /**
     * ajax 返回的数据
     */
    private Object data;

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
