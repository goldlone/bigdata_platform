package cn.goldlone.bigdata_platform.configuration;

import cn.goldlone.bigdata_platform.interceptor.LoginRequiredInterceptor;
import cn.goldlone.bigdata_platform.interceptor.PassportInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.Arrays;

/**
 * @author Created by CN on 2018/08/9/0009 11:59 .
 */
@Component
public class ForumConfiguration implements WebMvcConfigurer {

    @Autowired
    PassportInterceptor passportInterceptor;

    @Autowired
    LoginRequiredInterceptor loginRequiredInterceptor;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(passportInterceptor);
        registry.addInterceptor(loginRequiredInterceptor)
            .addPathPatterns("/*")
            .excludePathPatterns(Arrays.asList("/register", "/login", "/error"));
    }
}
