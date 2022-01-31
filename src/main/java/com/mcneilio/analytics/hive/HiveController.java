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
        System.out.println("Firing up on localhost port 8080, hardcoded.");
        HiveConnector hive = HiveConnector.getConnector();
        String viewTemplate = getTemplate();
        Undertow server = Undertow.builder()
                .addHttpListener(8080, "localhost")
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
                                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                                    exchange.getResponseSender().send("List schemas for a specific database: " +
                                            params.getParameters().get("database") + "\n");
                                    exchange.getResponseSender().close();
                                })
                                .get("/{db}/{tableName}", exchange -> {
                                    exchange.getResponseHeaders().put(new HttpString("Access-Control-Allow-Origin"), "*");
                                    PathTemplateMatch params = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY);
                                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                                    exchange.getResponseSender().send(hive.getSchema(params.getParameters().get("db"),
                                            params.getParameters().get("tableName")) + "\n");
                                    exchange.getResponseSender().close();
                                })
                                .put("/{db}/{tableName}", exchange -> {
                                    PathTemplateMatch params = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY);
                                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                                    exchange.getRequestReceiver().receiveFullBytes((e, m) -> {
                                        hive.setSchema(params.getParameters().get("db"),
                                                params.getParameters().get("tableName"), new String(m));
                                    });
                                    exchange.getResponseSender().close();
                                })
                        )
                        .addPrefixPath("/ui", Handlers.routing()
                                .get("/{db}/{tableName}", exchange -> {
                                    PathTemplateMatch params = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY);
                                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/html");
                                    String renderedView = viewTemplate
                                            .replace("{{db}}", params.getParameters().get("db"))
                                            .replace("{{tableName}}", params.getParameters().get("tableName"));
                                    exchange.getResponseSender().send(renderedView);
                                }))
                ).build();
        server.start();
    }

    private static String getTemplate() {
        // TODO: this fails to read in IDE because resource files are annoying
        HiveController obj = new HiveController();
        InputStream in = obj.getClass().getClassLoader().getResourceAsStream("ui/HiveViewSchema.html");
        return new BufferedReader(new InputStreamReader(in)).lines().collect(Collectors.joining("\n"));
    }
}