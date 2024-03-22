import com.google.gson.*;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpHandlers;
import com.sun.net.httpserver.HttpServer;
import org.json.JSONObject;

import java.io.*;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

import static Utility.HttpUtility.*;


public class QueryEquivalenceApp {

    private static final InetSocketAddress LOOPBACK_ADDR =
            new InetSocketAddress(InetAddress.getLoopbackAddress(), 8080);

    private ExecutorService pool;
    private Map<UUID, Future<Job<QueryEquivalenceJobResult>>> pendingJobs;
    private final HttpServer server;

    private final Gson gson = new GsonBuilder().serializeNulls().create();

    private final HttpHandler testHandler = HttpHandlers.of(
            STATUS_OK,
            Headers.of(HEADER_CONTENT_TYPE, String.format("application/text; charset=%s", CHARSET)),
            "Hello world!"
    );

    private final HttpHandler notAllowedHandler = HttpHandlers.of(
            STATUS_METHOD_NOT_ALLOWED,
            Headers.of(HEADER_ALLOW, METHOD_GET + "," + METHOD_POST),
            ""
    );

    private final HttpHandler qeHandler = HttpHandlers.handleOrElse(
            IS_GET_OR_POST,
            he -> {
                try (he) {
                    String requestMethod = he.getRequestMethod().toUpperCase();
                    InputStream in = he.getRequestBody();
                    OutputStream out = he.getResponseBody();
                    Map<String, String> params = queryToMap(he.getRequestURI().getQuery());
                    Headers headers = he.getResponseHeaders();
                    headers.set(HEADER_CONTENT_TYPE, String.format("%s; charset=%s", CONTENT_TYPE_JSON, CHARSET));
                    he.sendResponseHeaders(STATUS_OK, 0);

                    if (requestMethod.equals(METHOD_GET)) {     // GET
                        // Fetch id from parameters
                        UUID id = UUID.fromString(params.get("id"));

                        // Try to get result
                        QueryEquivalenceJob job = new QueryEquivalenceJob(id);    // return this if still in progress
                        Future<Job<QueryEquivalenceJobResult>> future = this.pendingJobs.get(id);
                        if (future.isDone()) {
                            this.pendingJobs.remove(id);
                            try {
                                job = (QueryEquivalenceJob) future.get();

                            } catch (InterruptedException | ExecutionException e) {
                                e.printStackTrace();
                            }
                        }
                        sendJson(out, job, QueryEquivalenceJob.class);

                    } else {        // POST

                        // Obtain qe job from body (json)
                        QueryEquivalenceJob job = this.gson.fromJson(
                                new InputStreamReader(in, CHARSET),
                                QueryEquivalenceJob.class
                        );
                        job.randomId();

                        // Submit and track job
                        Future<Job<QueryEquivalenceJobResult>> future = this.pool.submit(job);
                        this.pendingJobs.put(job.getId(), future);

                        // Send response
                        sendJson(out, job, QueryEquivalenceJob.class);
                    }
                }

            },
            notAllowedHandler
    );


    public QueryEquivalenceApp() throws IOException {
        this.pool = Executors.newCachedThreadPool();
        this.pendingJobs = new ConcurrentHashMap<>();
        this.server = HttpServer.create(LOOPBACK_ADDR, 0);
        this.server.createContext("/test", this.testHandler);
        this.server.createContext("/qe", this.qeHandler);
        this.server.setExecutor(this.pool);
    }


    public void start() {
        System.out.println("Starting the server at " + server.getAddress() + " ... ");
        server.start();
        System.out.println("Server started!");
    }

    public void stop() {
        server.stop(1);
    }

    private void sendJson(OutputStream out, Object obj, Type type) throws IOException {
        JsonElement json = this.gson.toJsonTree(obj, type);
        out.write(this.gson.toJson(json).getBytes(CHARSET));
    }


    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        QueryEquivalenceApp app = new QueryEquivalenceApp();
        app.start();

        // http client
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8080/test"))
                .GET()
                .build();
        client.sendAsync(request, BodyHandlers.ofString())
                .thenApply(HttpResponse::body)
                .thenAccept(System.out::println)
                .join();


        // Test qe
        File f = new File("testData/calcite_tests.json");
        JsonParser parser = new JsonParser();
        JsonArray array = parser.parse(new FileReader(f)).getAsJsonArray();

        // Make requests for all test cases
        Thread[] threads = new Thread[array.size()];
        String id = UUID.randomUUID().toString();
        for(int i=0; i < array.size(); i++){
            JsonObject testCase = array.get(i).getAsJsonObject();
            threads[i] = new Thread(() -> {
                String sql1 = testCase.get("q1").getAsString();
                String sql2 = testCase.get("q2").getAsString();
                String name = testCase.get("name").getAsString();
                Map<String, String> bodyMap = Map.of(
                        "sql1", sql1,
                        "sql2", sql2,
                        "name", name,
                        "id", id
                );
                String json = new Gson().toJson(bodyMap);

                // Http request (POST)
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:8080/qe"))
                        .POST(BodyPublishers.ofString(json, CHARSET))
                        .header(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON)
                        .build();
                HttpResponse<String> response = null;
                try {
                    response = client.sendAsync(req, BodyHandlers.ofString(CHARSET)).get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                JSONObject jo = new JSONObject(response.body());
                String id2 = jo.getString("id");

                // Get result
                req = HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:8080/qe?id=" + id2))
                        .GET()
                        .build();
                while (true) {
                    String body = null;
                    try {
                        body = client.sendAsync(req, BodyHandlers.ofString(CHARSET))
                                .thenApply(HttpResponse::body).get();
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                    jo = new JSONObject(body);

                    if (!jo.isNull("result")) {
                        JSONObject result = jo.getJSONObject("result");
                        System.out.println("Result for " + name + ": " + result);
                        break;
                    }
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            threads[i].start();
        }

        // w8 for threads to finish
        for (Thread thread : threads) {
            thread.join();
        }
        app.stop();

    }

}
