package com.mcneilio.analytics.hive;

import com.google.gson.Gson;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.List;

public class HiveConnector {
    private static HiveConnector connector = null;

    public static HiveConnector getConnector() {
        if(connector == null) {
            connector = new HiveConnector();
        }
        return connector;
    }

    private HiveConnector() {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.local", "false");

        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, System.getenv("HIVE_URL"));
        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
        try {
            this.client = new HiveMetaStoreClient(hiveConf, null);
        } catch (MetaException e) {
            e.printStackTrace();
        }
        this.gson = new Gson();
    }

    public String getSchema(String db, String tableName) {
        try {
            Fields fields = new Fields();
            fields.fields = this.client.getSchema(db, tableName);
            return gson.toJson(fields);
        } catch (Exception e) {
            e.printStackTrace();
            // TODO: error handling (currently crashes app on unknown schema)
            System.exit(1);
            return null;
        }
    }

    public void setSchema(String db, String tableName, String fields) {
        List<FieldSchema> fieldList =  gson.fromJson(fields, Fields.class).fields;
        try {
            Table tbl = new Table(client.getTable(db, tableName));
            StorageDescriptor sd = tbl.getSd();
            sd.setCols(fieldList);
            client.alter_table(db, tableName, tbl);
        } catch (Exception e) {
            e.printStackTrace();
            // TODO: error handling
            System.exit(1);
        }
    }

    public String listTables(String db) {
        try {
            List<String> tables = client.getTables(db, "*");
            return gson.toJson(tables);
        }
        catch (Exception e) {
            e.printStackTrace();
            // TODO: error handling
            System.exit(1);
            return null;
        }
    }

    public String addTable(String db, String tbl) {
        Table table = gson.fromJson(tbl, Table.class);
        try {
            client.createTable(table);
        }
        catch (Exception e) {
            e.printStackTrace();
            // TODO: error handling
            System.exit(1);
        }
        return "";
    }

    private HiveMetaStoreClient client;
    private Gson gson;

    private class Fields {
        List<FieldSchema> fields;

        Fields() {
            fields = new ArrayList<>();
        }
    }
}
