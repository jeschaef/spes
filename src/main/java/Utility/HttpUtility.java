package Utility;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.Request;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class HttpUtility {

    public static final String HEADER_ALLOW = "Allow";
    public static final String METHOD_GET = "GET";
    public static final String METHOD_POST = "POST";
    public static final int STATUS_OK = 200;
    public static final int STATUS_METHOD_NOT_ALLOWED = 405;
    public static final int NO_RESPONSE_LENGTH = -1;
    public static final String HEADER_CONTENT_TYPE = "Content-Type";
    public static final String CONTENT_TYPE_STRING = "application/text";
    public static final String CONTENT_TYPE_JSON = "application/json";
    public static final Charset CHARSET = StandardCharsets.UTF_8;

    public static final String JOB_STATUS_DONE = "done";
    public static final String JOB_STATUS_PROGRESS = "progress";

    public static final Predicate<Request> IS_GET = r -> r.getRequestMethod().equalsIgnoreCase(METHOD_GET);
    public static final Predicate<Request> IS_POST = r -> r.getRequestMethod().equalsIgnoreCase(METHOD_POST);
    public static final Predicate<Request> IS_GET_OR_POST = IS_GET.or(IS_POST);

    public static void sendOkResponse(HttpExchange he) throws IOException {
        Headers headers = he.getResponseHeaders();
        headers.set(HEADER_CONTENT_TYPE, String.format("%s; charset=%s", CONTENT_TYPE_STRING, CHARSET));
        he.sendResponseHeaders(STATUS_OK, NO_RESPONSE_LENGTH);
    }

    public static void sendOkResponse(HttpExchange he, String body) throws IOException {
        Headers headers = he.getResponseHeaders();
        headers.set(HEADER_CONTENT_TYPE, String.format("%s; charset=%s", CONTENT_TYPE_STRING, CHARSET));
        byte[] rawBody = body.getBytes(CHARSET);
        he.sendResponseHeaders(STATUS_OK, rawBody.length);
        he.getResponseBody().write(rawBody);
    }

    public static void sendOkResponse(HttpExchange he, JsonElement body) throws IOException {
        Headers headers = he.getResponseHeaders();
        headers.set(HEADER_CONTENT_TYPE, String.format("%s; charset=%s", CONTENT_TYPE_JSON, CHARSET));
        he.sendResponseHeaders(STATUS_OK, 0);
        OutputStreamWriter writer = new OutputStreamWriter(he.getResponseBody(), CHARSET);
        new Gson().toJson(body, writer);
    }




    public static Map<String, String> queryToMap(String query) {
        if(query == null) {
            return null;
        }
        Map<String, String> result = new HashMap<>();
        for (String param : query.split("&")) {
            String[] entry = param.split("=");
            if (entry.length > 1) {
                result.put(entry[0], entry[1]);
            }else{
                result.put(entry[0], "");
            }
        }
        return result;
    }

}
