package com.byf.flumeinterceptor.userdefine;

import com.byf.flumeinterceptor.utils.LogUtil;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class LogETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //获取事件内容
        byte[] eventBody = event.getBody();
        //将字节数组的内容转换为String字符串
        String eventString = new String(eventBody, Charset.forName("UTF-8"));

        //判断日志格式是否正确
        if (LogUtil.isValidate(eventString)) {
            return event;
        }

        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        //创建一个集合接收多个事件
        List<Event> eventList = new ArrayList<>();
        //遍历集合，调用单个event方法
        for (Event event : events) {
            //判断每一个event是否符合要求
            Event e = intercept(event);
            if (e != null) {
                eventList.add(e);
            }
        }
        //返回符合要求的event集合
        return eventList;
    }

    @Override
    public void close() {

    }

    //创建静态内部类Builder
    public static class Builder implements Interceptor.Builder{
        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
