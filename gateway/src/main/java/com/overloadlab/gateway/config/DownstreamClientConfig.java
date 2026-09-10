package com.overloadlab.gateway.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.util.Timeout;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration
public class DownstreamClientConfig {

    /** Stand-in for "no timeout" at stage s0. Long enough to be indistinguishable from infinite. */
    private static final Timeout EFFECTIVELY_INFINITE = Timeout.ofHours(1);

    @Bean
    public PoolingHttpClientConnectionManager connectionManager(OverloadProperties props,
                                                               MeterRegistry registry) {
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(props.getHttpPoolSize());
        cm.setDefaultMaxPerRoute(props.getHttpPoolSize());

        // Gauges are registered here rather than in a second @Bean: two beans of the same
        // type would make the injection in httpClient() ambiguous.
        Gauge.builder("overload.http.pool.leased", cm, m -> m.getTotalStats().getLeased())
                .description("HTTP connections currently leased").register(registry);
        Gauge.builder("overload.http.pool.pending", cm, m -> m.getTotalStats().getPending())
                .description("Threads waiting for an HTTP connection").register(registry);
        Gauge.builder("overload.http.pool.available", cm, m -> m.getTotalStats().getAvailable())
                .description("Idle HTTP connections").register(registry);

        boolean timeouts = props.getHttp().getTimeouts().isEnabled();
        cm.setDefaultConnectionConfig(ConnectionConfig.custom()
                .setConnectTimeout(timeouts
                        ? Timeout.ofMilliseconds(props.getHttp().getTimeouts().getConnectMs())
                        : EFFECTIVELY_INFINITE)
                .build());
        return cm;
    }

    @Bean
    public CloseableHttpClient httpClient(PoolingHttpClientConnectionManager cm, OverloadProperties props) {
        boolean timeouts = props.getHttp().getTimeouts().isEnabled();
        RequestConfig requestConfig = RequestConfig.custom()
                .setResponseTimeout(timeouts
                        ? Timeout.ofMilliseconds(props.getHttp().getTimeouts().getResponseMs())
                        : EFFECTIVELY_INFINITE)
                .setConnectionRequestTimeout(timeouts
                        ? Timeout.ofMilliseconds(props.getHttp().getTimeouts().getLeaseMs())
                        : EFFECTIVELY_INFINITE)
                .build();
        return HttpClients.custom()
                .setConnectionManager(cm)
                .setDefaultRequestConfig(requestConfig)
                .build();
    }

    @Bean
    public RestClient downstreamRestClient(CloseableHttpClient httpClient) {
        return RestClient.builder()
                .requestFactory(new HttpComponentsClientHttpRequestFactory(httpClient))
                .build();
    }

}
