package com.mcneilio.analytics.hive;

import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import io.undertow.util.PathTemplateMatch;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class HiveController {
    static public void main(String[] args) {
        String listenAddr = System.getenv("LISTEN_ADDR");
        int listenPort = Integer.parseInt(System.getenv("LISTEN_PORT"));
        System.out.println("Firing up on " + listenAddr + " port " + listenPort + ", hardcoded.");
        HiveConnector hive = HiveConnector.getConnector();
        String viewTemplate = getTemplate();
        String viewTemplate2 = getTemplate2();
        String viewTemplate3 = getTemplate3();
        Undertow server = Undertow.builder()
                .addHttpListener(listenPort, listenAddr)
                .setHandler(Handlers.path()
                        .addExactPath("/", exchange -> {
                            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                            exchange.getResponseSender().send("Root path\n");
                        })
                        .addPrefixPath("/schemas", Handlers.routing()
                                .get("/", exchange -> {
                                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                                    exchange.getResponseSender().send("List the databases\n");
                                    exchange.getResponseSender().close();
                                })
                                .get("/{database}", exchange -> {
                                    PathTemplateMatch params = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY);
                                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                                    exchange.getResponseSender().send(hive.listTables(params.getParameters().get("database")) + "\n");
                                    exchange.getResponseSender().close();
                                })
                                .post("/{database}", exchange -> {
                                    PathTemplateMatch params = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY);
                                    exchange.getRequestReceiver().receiveFullBytes((e, m) -> {
                                        hive.addTable(params.getParameters().get("db"), new String(m));
                                        exchange.getResponseSender().close();
                                    });
                                })
                                .get("/{database}/{tableName}", exchange -> {
                                    PathTemplateMatch params = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY);
                                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                                    if(!params.getParameters().get("tableName").isEmpty())
                                        exchange.getResponseSender().send(hive.getTable(params.getParameters().get("database"), params.getParameters().get("tableName")) + "\n");
                                    else
                                        exchange.getResponseSender().send("{}\n");
                                    exchange.getResponseSender().close();
                                })
                                .put("/{database}/{tableName}", exchange -> {
                                    PathTemplateMatch params = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY);
                                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                                    exchange.getRequestReceiver().receiveFullBytes((e, m) -> {
                                        hive.updateTable(params.getParameters().get("database"),
                                                params.getParameters().get("tableName"), new String(m));
                                        exchange.getResponseSender().send("probably accepted it" + "\n");
                                        exchange.getResponseSender().close();
                                    });
                                })
                        )
                        .addPrefixPath("/ui", Handlers.routing()
                                .get("/{db}/{tableName}", exchange -> {
                                    PathTemplateMatch params = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY);
                                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/html");
                                    String renderedView;
                                    if(params.getParameters().get("tableName").isEmpty()) {
                                        renderedView = viewTemplate3
                                                .replace("{{db}}", params.getParameters().get("db"))
                                                .replace("{{url_prefix}}", System.getenv("URL_PREFIX"));
                                    }
                                    else {
                                        renderedView = viewTemplate
                                                .replace("{{db}}", params.getParameters().get("db"))
                                                .replace("{{tableName}}", params.getParameters().get("tableName"))
                                                .replace("{{url_prefix}}", System.getenv("URL_PREFIX"));
                                    }
                                    exchange.getResponseSender().send(renderedView);
                                })
                                .get("/{db}", exchange -> {
                                    PathTemplateMatch params = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY);
                                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/html");
                                    String renderedView = viewTemplate2
                                            .replace("{{db}}", params.getParameters().get("db"))
                                            .replace("{{url_prefix}}", System.getenv("URL_PREFIX"));
                                    exchange.getResponseSender().send(renderedView);
                                })
                        )
                ).build();
        server.start();
    }

    private static String getTemplate() {
        HiveController obj = new HiveController();
        InputStream in = obj.getClass().getClassLoader().getResourceAsStream("ui/HiveViewSchema.html");
        return new BufferedReader(new InputStreamReader(in)).lines().collect(Collectors.joining("\n"));
    }

    private static String getTemplate2() {
        HiveController obj = new HiveController();
        InputStream in = obj.getClass().getClassLoader().getResourceAsStream("ui/HiveViewTables.html");
        return new BufferedReader(new InputStreamReader(in)).lines().collect(Collectors.joining("\n"));
    }

    private static String getTemplate3() {
        HiveController obj = new HiveController();
        InputStream in = obj.getClass().getClassLoader().getResourceAsStream("ui/HiveCreateTable.html");
        return new BufferedReader(new InputStreamReader(in)).lines().collect(Collectors.joining("\n"));
    }
}
