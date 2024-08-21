package com.kv.config;

import lombok.extern.log4j.Log4j2;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.util.StopWatch;

import java.io.IOException;

@Configuration
@Log4j2
public class RestTemplateInterceptor implements ClientHttpRequestInterceptor {
    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution) throws IOException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        ClientHttpResponse response = execution.execute(request, body);
        stopWatch.stop();
        log.info("RestInterceptor-> HttpStatusCode: {}, URI: {} and took {} seconds", response.getStatusCode(), request.getURI(), stopWatch.getTotalTimeSeconds());

//        Integer count = (Integer) PerformanceLogContext.getPerformanceLogContext().get("API_CALL_COUNT");
//        PerformanceLogContext.setPerformanceLogContext("API_CALL_COUNT", ++count);

        return response;
    }
}
