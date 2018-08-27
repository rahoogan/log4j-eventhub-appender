package com.rahoogan.http;

import java.io.IOException;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.HttpResponse;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

public class HttpPostMethodThread implements Runnable {

    private final CloseableHttpClient httpClient;
    private ErrorHandler errorHandler = null;
    private HttpPost post = null;

    public HttpPostMethodThread(HttpPost post, ErrorHandler errorHandler) {
        this.httpClient = HttpClientBuilder.create().build();
        this.errorHandler = errorHandler;
        this.post = post;
    }
    public void run() {
        try {
            try {
                HttpResponse response = httpClient.execute(post);
                System.out.println(response.getStatusLine().getStatusCode());
                HttpEntity entity = response.getEntity();
                String content = EntityUtils.toString(entity);
                System.out.println(content);
            } catch (IOException e) {
                errorHandler.error(e.toString());
            } catch (NullPointerException n) {
                errorHandler.error(n.toString());
            } finally {
                this.httpClient.close();
            }
        } catch (IOException e) {
            errorHandler.error(e.toString());
        }
    }
}