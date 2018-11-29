package cn.goldlone.bigdata_platform.utils;

import cn.goldlone.bigdata_platform.model.Result;
import cn.goldlone.bigdata_platform.model.ResultCode;

/**
 * @author Created by CN on 2018/08/9/0009 16:16 .
 */
public class ResultUtil {

    public static Result success() {
        return success(null);
    }

    public static Result success(String msg) {
        return success(msg, null);
    }

    public static Result success(String msg, Object data) {
        return new Result(ResultCode.SUCCESS.getCode(), msg, data);
    }


    public static Result error(int code) {
        return error(code, null);
    }

    public static Result error(int code, String msg) {
        return error(code, msg, null);
    }

    public static Result error(int code, String msg, Object data) {
        return new Result(code, msg, data);
    }

    public static Result errorUserNotLogin() {
        return error(ResultCode.USER_NOT_LOGIN.getCode(), ResultCode.USER_NOT_LOGIN.getDescription());
    }
}
