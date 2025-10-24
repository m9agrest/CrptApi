package ru.mina987.CrptApi;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

public class CrptApi {
    public static void main(String[] args) {
    }

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final Deque<Long> hits = new ArrayDeque<>();
    private final Object rateLock = new Object();
    private final long windowNanos;
    private final int requestLimit;

    public CrptApi(TimeUnit timeUnit, int requestLimit){
        if (requestLimit <= 0) throw new IllegalArgumentException("requestLimit must be > 0"); //todo лучше не пихать в конструктор, а проверять извне или из request

        this.requestLimit = requestLimit;
        this.windowNanos = timeUnit.toNanos(1);
    }

    public HttpResponse<String> request(String token, RequestPayload data){
        waitLimit();

        return send(token, data);
    }


    private void waitLimit() {
        boolean interrupted = false;
        synchronized (rateLock) {
            for (;;) {
                long now = System.nanoTime();
                while (!hits.isEmpty() && now - hits.peekFirst() >= windowNanos) {
                    hits.removeFirst();
                }
                if (hits.size() < requestLimit) {
                    hits.addLast(now);
                    if (interrupted) Thread.currentThread().interrupt(); // восстановим флаг
                    return;
                }
                long waitNanos = windowNanos - (now - hits.peekFirst());
                long ms = TimeUnit.NANOSECONDS.toMillis(waitNanos);
                int ns = (int) (waitNanos - TimeUnit.MILLISECONDS.toNanos(ms));
                try {
                    rateLock.wait(ms, ns);
                } catch (InterruptedException ie) {
                    // игнорируем, но помним, что нас прерывали
                    interrupted = true;
                    // продолжаем ждать, пока не получим слот
                }
            }
        }
    }



    public HttpResponse<String> request(String token, Object document, String signature, ProductGroup product_group){
        return request(token, new RequestPayload(document, signature, product_group));
    }

    private HttpResponse<String> send(String token, RequestPayload data){
        try{
            //в data предустановлены поля для запроса api/v3/lk/documents/create
            //можно так-же использовать api/v3/lk/documents/send

            return sendPost("https://ismp.crpt.ru/api/v3/lk/documents/create?pg=" + data.getGroup(), token, data);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private HttpResponse<String> sendPost(String apiUrl, String token, AbstractJson body) throws IOException, InterruptedException {
        if(token == null){
            throw new NullPointerException("token productDocument is null");
        }
        HttpRequest request = HttpRequest.newBuilder()
                                         .uri(URI.create(apiUrl))
                                         .header("Content-Type", "application/json")
                                         .header("Authorization", token)
                                         .POST(HttpRequest.BodyPublishers.ofString(body.toJson()))
                                         .build();

        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }


    public enum DocumentFormat{MANUAL, XML, CSV}
    public enum ProductGroup{clothes, shoes, tobacco, perfumery, tires, electronics, pharma, milk, bicycle, wheelchairs}


    @Getter
    @Setter
    public static class RequestPayload extends AbstractJson {
        @JsonProperty("product_document")
        @JsonSerialize(using = Base64JsonSerializer.class)
        private Object document;

        @JsonProperty("signature")
        private String signature;

        @JsonProperty("document_format")
        private DocumentFormat document_format = DocumentFormat.MANUAL;//судя по type = LP_INTRODUCE_GOODS, формат должен быть JSON / MANUAL

        @JsonProperty("type")
        private String type = "LP_INTRODUCE_GOODS";

        @JsonProperty("product_group")
        private ProductGroup group;

        public RequestPayload(Object document, String signature, ProductGroup group) {
            this.document = document;
            this.signature = signature;
            this.group = group;
        }
    }

    public static abstract class AbstractJson{
        static final private ObjectMapper objectMapper = new ObjectMapper();
        @JsonIgnore
        public String toJson() throws JsonProcessingException {
            return objectMapper.writeValueAsString(this);
        }
    }

    public static final class Base64JsonSerializer extends JsonSerializer<Object> {
        private static final ObjectMapper OM = new ObjectMapper();

        @Override
        public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            ObjectMapper active = (ObjectMapper) gen.getCodec();
            String json = (active != null ? active : OM).writeValueAsString(value);
            String b64  = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
            gen.writeString(b64); // в итоговом JSON это будет строка
        }
    }
}