package ru.oleg520.selsup;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CrptApi {
    private final TimeUnit timeUnit;
    private final int requestLimit;
    private final Semaphore semaphore;
    private final BlockingQueue<Instant> requestTimestamps;
    private final HttpClient httpClient;
    private final ObjectMapper jsonMapper;

    // константы захардкодил, думаю что эти значения должны быть указаны в конфигах
    private static final String AUTH_TOKEN = "token"; // токен авторизации
    private static final String SIGNATURE = "signature"; // <Открепленная подпись в base64>
    private static final String API_URL = "https://ismp.crpt.ru/api/v3/lk/documents/create";

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        if (requestLimit <= 0) {
            throw new IllegalArgumentException("RequestLimit должен быть положительным");
        }
        this.timeUnit = Objects.requireNonNull(timeUnit, "TimeUnit не может быть null");
        this.requestLimit = requestLimit;
        this.semaphore = new Semaphore(requestLimit);
        this.requestTimestamps = new LinkedBlockingQueue<>(requestLimit);
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();
        this.jsonMapper = new ObjectMapper();
    }

    public static void main(String[] args) {
        // Создаем API с лимитом 2 запроса в секунду
        CrptApi api = new CrptApi(TimeUnit.SECONDS, 2);
        String signature = Base64.getEncoder().encodeToString(SIGNATURE.getBytes());

        // Создаем документ
        Product product = new Product("cert", new Date(), "certNum", "123123", "232323", new Date(), "454545", "1234435", "9876");
        ProductDocument productDocument = new ProductDocument(new ProductDocument.Description("12345678"),"123", "status","type",false, "12345678", "23456789", "12321323", new Date(), "prodType", List.of(product), new Date(), "12344");
        Document doc = new Document("MANUAL", productDocument, "milk", signature, "LP_INTRODUCE_GOODS");

        // Для имитации многопоточной работы
        int numberOfRequests = 10;
        for (int i = 0; i < numberOfRequests; i++) {
            final int n = i;
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        System.out.println("Отправка запроса " + n + " в " + Instant.now());
                        api.createDocument(doc,signature);
                        System.out.println("Запрос " + n + " успешно выполнен в " + Instant.now());
                    } catch (InterruptedException | IOException | ApiException e) {
                        System.err.println("Ошибка в запросе " + n + ": " + e.getMessage());
                    }
                }
            }, " " + i);
            t.start();
        }
//        ExecutorService executor = Executors.newFixedThreadPool(4);
//        for (int i = 0; i < numberOfRequests; i++) {
//            final int n = i + 1;
//            executor.submit(() -> {
//                try {
//                    System.out.println("Отправка запроса " + n + " в " + Instant.now());
//                    api.createDocument(doc, signature);
//                    System.out.println("Запрос " + n + " успешно выполнен в " + Instant.now());
//                } catch (InterruptedException | ApiException | IOException e) {
//                    System.err.println("Ошибка в запросе " + n + ": " + e.getMessage());
//                }
//            });
//        }
//        executor.shutdown();
    }

    // Метод создания документа для ввода в оборот товара, произведенного в РФ
    public void createDocument(Document document, String signature) throws InterruptedException, IOException, ApiException {
        try {
            semaphore.acquire();
            waitForAvailableSlot();
            requestTimestamps.put(Instant.now());

            HttpRequest request = buildRequest(document, signature);
            HttpResponse<String> response = sendRequest(request);

            if (response.statusCode() != 200) {
                throw new ApiException("API error: " + response.body());
            }
        } finally {
            semaphore.release();
        }
    }

    // формирование запроса
    private HttpRequest buildRequest(Document document, String signature) throws JsonProcessingException {
        String product_document = Base64.getEncoder().encodeToString(toJson(document.getProductDocument()).getBytes());
        String requestBody = String.format("{ \"product_document\": \"%s\", \"document_format\": \"%s\", \"type\": \"%s\", \"signature\": \"%s\" }",
                product_document, document.getDocumentFormat(), document.getType(), signature);
        return HttpRequest.newBuilder()
                .uri(URI.create(API_URL + "?pg=" + document.getProductGroup()))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + AUTH_TOKEN)
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();
    }

    private String toJson(Object document) throws JsonProcessingException {
        return jsonMapper.writeValueAsString(document);
    }

    private HttpResponse<String> sendRequest(HttpRequest request) throws IOException, InterruptedException {
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    // ожидание освобождения слота для отправки следующего запроса
    private void waitForAvailableSlot() throws InterruptedException {
        while (true) {
            synchronized (requestTimestamps) {
                cleanOldRequests();

                if (requestTimestamps.size() < requestLimit) {
                    return;
                }

                Instant oldest = requestTimestamps.peek();
                long waitTime = timeUnit.toMillis(1) - (Instant.now().toEpochMilli() - oldest.toEpochMilli());

                if (waitTime > 0) {
                    Thread.sleep(waitTime);
                }
            }
        }
    }

    // очистка старых запросов
    private void cleanOldRequests() {
        Instant cutoff = Instant.now().minusMillis(timeUnit.toMillis(1));
        while (requestTimestamps.peek() != null && requestTimestamps.peek().isBefore(cutoff)) {
            requestTimestamps.poll();
        }
    }
}

class Document {
    @JsonProperty("document_format")
    private String documentFormat;

    @JsonProperty("product_document")
    private ProductDocument productDocument;

    @JsonProperty("product_group")
    private String productGroup;

    @JsonProperty("signature")
    private String signature;

    @JsonProperty("type")
    private String type;

    public Document(String documentFormat, ProductDocument productDocument, String productGroup, String signature, String type) {
        this.documentFormat = documentFormat;
        this.productDocument = productDocument;
        this.productGroup = productGroup;
        this.signature = signature;
        this.type = type;
    }

    public String getDocumentFormat() {
        return documentFormat;
    }

    public ProductDocument getProductDocument() {
        return productDocument;
    }

    public String getProductGroup() {
        return productGroup;
    }

    public String getSignature() {
        return signature;
    }

    public String getType() {
        return type;
    }
}

class ProductDocument {
    @JsonProperty("description")
    private Description description;
    @JsonProperty("doc_id")
    private String docId;
    @JsonProperty("doc_status")
    private String docStatus;
    @JsonProperty("doc_type")
    private String docType;
    @JsonProperty("importRequest")
    private boolean importRequest;
    @JsonProperty("owner_inn")
    private String ownerInn;
    @JsonProperty("participant_inn")
    private String participantInn;
    @JsonProperty("producer_inn")
    private String producerInn;
    @JsonProperty("production_date")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    private Date productionDate;
    @JsonProperty("production_type")
    private String productionType;
    @JsonProperty("products")
    private List<Product> products;
    @JsonProperty("reg_date")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    private Date regDate;
    @JsonProperty("reg_number")
    private String regNumber;

    public ProductDocument(){}
    public ProductDocument(Description description, String docId, String docStatus, String docType, boolean importRequest, String ownerInn, String participantInn, String producerInn, Date productionDate, String productionType, List<Product> products, Date regDate, String regNumber) {
        this.description = description;
        this.docId = docId;
        this.docStatus = docStatus;
        this.docType = docType;
        this.importRequest = importRequest;
        this.ownerInn = ownerInn;
        this.participantInn = participantInn;
        this.producerInn = producerInn;
        this.productionDate = productionDate;
        this.productionType = productionType;
        this.products = products;
        this.regDate = regDate;
        this.regNumber = regNumber;
    }

    static class Description {
        private String participantInn;

        public Description(String participantInn) {
            this.participantInn = participantInn;
        }

        public String getParticipantInn() {
            return participantInn;
        }
    }

    public Description getDescription() {
        return description;
    }

    public void setDescription(Description description) {
        this.description = description;
    }

    public String getDocId() {
        return docId;
    }

    public void setDocId(String docId) {
        this.docId = docId;
    }

    public String getDocStatus() {
        return docStatus;
    }

    public void setDocStatus(String docStatus) {
        this.docStatus = docStatus;
    }

    public String getDocType() {
        return docType;
    }

    public void setDocType(String docType) {
        this.docType = docType;
    }

    public boolean isImportRequest() {
        return importRequest;
    }

    public void setImportRequest(boolean importRequest) {
        this.importRequest = importRequest;
    }

    public String getOwnerInn() {
        return ownerInn;
    }

    public void setOwnerInn(String ownerInn) {
        this.ownerInn = ownerInn;
    }

    public String getParticipantInn() {
        return participantInn;
    }

    public void setParticipantInn(String participantInn) {
        this.participantInn = participantInn;
    }

    public String getProducerInn() {
        return producerInn;
    }

    public void setProducerInn(String producerInn) {
        this.producerInn = producerInn;
    }

    public Date getProductionDate() {
        return productionDate;
    }

    public void setProductionDate(Date productionDate) {
        this.productionDate = productionDate;
    }

    public String getProductionType() {
        return productionType;
    }

    public void setProductionType(String productionType) {
        this.productionType = productionType;
    }

    public List<Product> getProducts() {
        return products;
    }

    public void setProducts(List<Product> products) {
        this.products = products;
    }

    public Date getRegDate() {
        return regDate;
    }

    public void setRegDate(Date regDate) {
        this.regDate = regDate;
    }

    public String getRegNumber() {
        return regNumber;
    }

    public void setRegNumber(String regNumber) {
        this.regNumber = regNumber;
    }
}

class Product {
    @JsonProperty("certificate_document")
    private String certificateDocument;

    @JsonProperty("certificate_document_date")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    private Date certificateDocumentDate;

    @JsonProperty("certificate_document_number")
    private String certificateDocumentNumber;

    @JsonProperty("owner_inn")
    private String ownerInn;

    @JsonProperty("producer_inn")
    private String producerInn;

    @JsonProperty("production_date")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    private Date productionDate;

    @JsonProperty("tnved_code")
    private String tnvedCode;

    @JsonProperty("uit_code")
    private String uitCode;

    @JsonProperty("uitu_code")
    private String uituCode;

    public Product(){}
    public Product(String certificateDocument, Date certificateDocumentDate, String certificateDocumentNumber, String ownerInn, String producerInn, Date productionDate, String tnvedCode, String uitCode, String uituCode) {
        this.certificateDocument = certificateDocument;
        this.certificateDocumentDate = certificateDocumentDate;
        this.certificateDocumentNumber = certificateDocumentNumber;
        this.ownerInn = ownerInn;
        this.producerInn = producerInn;
        this.productionDate = productionDate;
        this.tnvedCode = tnvedCode;
        this.uitCode = uitCode;
        this.uituCode = uituCode;
    }

    public String getCertificateDocument() {
        return certificateDocument;
    }

    public void setCertificateDocument(String certificateDocument) {
        this.certificateDocument = certificateDocument;
    }

    public String getCertificateDocumentNumber() {
        return certificateDocumentNumber;
    }

    public void setCertificateDocumentNumber(String certificateDocumentNumber) {
        this.certificateDocumentNumber = certificateDocumentNumber;
    }

    public String getOwnerInn() {
        return ownerInn;
    }

    public void setOwnerInn(String ownerInn) {
        this.ownerInn = ownerInn;
    }

    public String getProducerInn() {
        return producerInn;
    }

    public void setProducerInn(String producerInn) {
        this.producerInn = producerInn;
    }

    public Date getCertificateDocumentDate() {
        return certificateDocumentDate;
    }

    public void setCertificateDocumentDate(Date certificateDocumentDate) {
        this.certificateDocumentDate = certificateDocumentDate;
    }

    public Date getProductionDate() {
        return productionDate;
    }

    public void setProductionDate(Date productionDate) {
        this.productionDate = productionDate;
    }

    public String getTnvedCode() {
        return tnvedCode;
    }

    public void setTnvedCode(String tnvedCode) {
        this.tnvedCode = tnvedCode;
    }

    public String getUitCode() {
        return uitCode;
    }

    public void setUitCode(String uitCode) {
        this.uitCode = uitCode;
    }

    public String getUituCode() {
        return uituCode;
    }

    public void setUituCode(String uituCode) {
        this.uituCode = uituCode;
    }
}

class ApiException extends Exception {
    public ApiException(String message) {
        super(message);
    }
}