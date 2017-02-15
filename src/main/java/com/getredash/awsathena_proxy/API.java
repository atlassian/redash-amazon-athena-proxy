package com.getredash.awsathena_proxy;

import com.google.gson.Gson;
import spark.Request;
import spark.Response;

import static spark.Spark.*;

import org.apache.log4j.Logger;

class QueryRequest {
    String athenaUrl;
    String awsAccessKey;
    String awsSecretKey;
    String s3StagingDir;
    String query;
}

public class API {

	private static final Logger LOG = Logger.getLogger(API.class);

    public static void main(String[] args) {
        String port = System.getenv("PORT");
        if (port != null) {
            port(Integer.valueOf(port));
        }

        Gson gson = new com.google.gson.GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
        post("/query", API::queryRequest, gson::toJson);
        get("/ping", (req, res) -> "PONG");
    }

    public static Object queryRequest(Request req, Response res) {
        LOG.info("Received query request: " + req.body());
        Gson gson = new com.google.gson.GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
        QueryRequest body = gson.fromJson(req.body(), QueryRequest.class);
        LOG.info("Sending request to Athena");
        Athena athena = new Athena(body.athenaUrl, body.awsAccessKey, body.awsSecretKey, body.s3StagingDir);

        try {
            Results results = athena.runQuery(body.query);
            LOG.info("Received response from Athena");
            return results;
        } catch (AthenaException e) {
            halt(400, e.getMessage());
        }
        return null;
    }
}
