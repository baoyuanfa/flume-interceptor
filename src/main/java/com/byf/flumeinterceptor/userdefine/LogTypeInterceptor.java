package com.byf.flumeinterceptor.userdefine;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogTypeInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //获取事件内容
        byte[] eventBody = event.getBody();
        //将字符数组的事件转换为String
        String eventString = new String(eventBody, Charset.forName("UTF-8"));

        String logType = "";

        if (eventString.contains("start")) {
            logType = "start";
        } else {
            logType = "event";
        }

        //获取事件头信息，并设置LogType
        Map<String, String> headers = event.getHeaders();
        headers.put("LogType", logType);

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        //创建集合接收事件
        List<Event> eventList = new ArrayList<>();

        //遍历事件集合，单独处理每个事件
        for (Event event : eventList) {
            Event e = intercept(event);
            eventList.add(e);
        }

        return eventList;
    }

    @Override
    public void close() {

    }

    //创建静态内部类
    public static class Builder implements Interceptor.Builder{
        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
