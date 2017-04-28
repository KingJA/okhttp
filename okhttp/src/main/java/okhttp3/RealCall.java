/*
 * Copyright (C) 2014 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package okhttp3;

import okhttp3.internal.NamedRunnable;
import okhttp3.internal.cache.CacheInterceptor;
import okhttp3.internal.connection.ConnectInterceptor;
import okhttp3.internal.connection.StreamAllocation;
import okhttp3.internal.http.BridgeInterceptor;
import okhttp3.internal.http.CallServerInterceptor;
import okhttp3.internal.http.RealInterceptorChain;
import okhttp3.internal.http.RetryAndFollowUpInterceptor;
import okhttp3.internal.platform.Platform;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static okhttp3.internal.platform.Platform.INFO;

final class RealCall implements Call {
    final OkHttpClient client;
    final RetryAndFollowUpInterceptor retryAndFollowUpInterceptor;
    final EventListener eventListener;

    /**
     * The application's original request unadulterated by redirects or auth headers.
     */
    final Request originalRequest;
    final boolean forWebSocket;

    // Guarded by this.
    private boolean executed;

    RealCall(OkHttpClient client, Request originalRequest, boolean forWebSocket) {
        final EventListener.Factory eventListenerFactory = client.eventListenerFactory();

        this.client = client;
        this.originalRequest = originalRequest;
        this.forWebSocket = forWebSocket;
        this.retryAndFollowUpInterceptor = new RetryAndFollowUpInterceptor(client, forWebSocket);

        // TODO(jwilson): this is unsafe publication and not threadsafe.
        this.eventListener = eventListenerFactory.create(this);
    }

    @Override
    public Request request() {
        return originalRequest;
    }

    /**
     * 检查这个 call 是否已经被执行了，每个 call 只能被执行一次，如果想要一个完全一样的 call，可以利用 call#clone
     * 方法进行克隆。
     * 利用 client.dispatcher().executed(this) 来进行实际执行，dispatcher 是刚才看到的 OkHttpClient.Builder 的成员之一，
     * 它的文档说自己是异步 HTTP 请求的执行策略，现在看来，同步请求它也有掺和。
     * 调用 getResponseWithInterceptorChain() 函数获取 HTTP 返回结果，从函数名可以看出，这一步还会进行一系列“拦截”操作。
     * 最后还要通知 dispatcher 自己已经执行完毕
     */
    @Override
    public Response execute() throws IOException {
        synchronized (this) {
            if (executed) throw new IllegalStateException("Already Executed");
            executed = true;
        }
        captureCallStackTrace();
        try {
            client.dispatcher().executed(this);
            Response result = getResponseWithInterceptorChain();
            if (result == null) throw new IOException("Canceled");
            return result;
        } finally {
            client.dispatcher().finished(this);
        }
    }

    private void captureCallStackTrace() {
        Object callStackTrace = Platform.get().getStackTraceForCloseable("response.body().close()");
        retryAndFollowUpInterceptor.setCallStackTrace(callStackTrace);
    }

    @Override
    public void enqueue(Callback responseCallback) {
        synchronized (this) {
            if (executed) throw new IllegalStateException("Already Executed");
            executed = true;
        }
        captureCallStackTrace();
        client.dispatcher().enqueue(new AsyncCall(responseCallback));
    }

    @Override
    public void cancel() {
        retryAndFollowUpInterceptor.cancel();
    }

    @Override
    public synchronized boolean isExecuted() {
        return executed;
    }

    @Override
    public boolean isCanceled() {
        return retryAndFollowUpInterceptor.isCanceled();
    }

    @SuppressWarnings("CloneDoesntCallSuperClone") // We are a final type & this saves clearing state.
    @Override
    public RealCall clone() {
        return new RealCall(client, originalRequest, forWebSocket);
    }

    StreamAllocation streamAllocation() {
        return retryAndFollowUpInterceptor.streamAllocation();
    }

    final class AsyncCall extends NamedRunnable {
        private final Callback responseCallback;

        AsyncCall(Callback responseCallback) {
            super("OkHttp %s", redactedUrl());
            this.responseCallback = responseCallback;
        }

        String host() {
            return originalRequest.url().host();
        }

        Request request() {
            return originalRequest;
        }

        RealCall get() {
            return RealCall.this;
        }

        @Override
        protected void execute() {
            boolean signalledCallback = false;
            try {
                Response response = getResponseWithInterceptorChain();
                if (retryAndFollowUpInterceptor.isCanceled()) {
                    signalledCallback = true;
                    responseCallback.onFailure(RealCall.this, new IOException("Canceled"));
                } else {
                    signalledCallback = true;
                    responseCallback.onResponse(RealCall.this, response);
                }
            } catch (IOException e) {
                if (signalledCallback) {
                    // Do not signal the callback twice!
                    Platform.get().log(INFO, "Callback failure for " + toLoggableString(), e);
                } else {
                    responseCallback.onFailure(RealCall.this, e);
                }
            } finally {
                client.dispatcher().finished(this);
            }
        }
    }

    /**
     * Returns a string that describes this call. Doesn't include a full URL as that might contain
     * sensitive information.
     */
    String toLoggableString() {
        return (isCanceled() ? "canceled " : "")
                + (forWebSocket ? "web socket" : "call")
                + " to " + redactedUrl();
    }

    String redactedUrl() {
        return originalRequest.url().redact();
    }

    Response getResponseWithInterceptorChain() throws IOException {
        // Build a full stack of interceptors.
        List<Interceptor> interceptors = new ArrayList<>();
        /*先获取用户配置的拦截器*/
        interceptors.addAll(client.interceptors());
        /*负责失败重试以及重定向*/
        interceptors.add(retryAndFollowUpInterceptor);
        /*负责把用户构造的请求转换为发送到服务器的请求、把服务器返回的响应转换为用户友好的响应*/
        interceptors.add(new BridgeInterceptor(client.cookieJar()));
        /*负责读取缓存直接返回、更新缓存*/
        interceptors.add(new CacheInterceptor(client.internalCache()));
        /*负责和服务器建立连接*/
        interceptors.add(new ConnectInterceptor(client));
        if (!forWebSocket) {
            interceptors.addAll(client.networkInterceptors());
        }
        /*负责向服务器发送请求数据、从服务器读取响应数据*/
        interceptors.add(new CallServerInterceptor(forWebSocket));

        Interceptor.Chain chain = new RealInterceptorChain(
                interceptors, null, null, null, 0, originalRequest);
        return chain.proceed(originalRequest);
    }
}
