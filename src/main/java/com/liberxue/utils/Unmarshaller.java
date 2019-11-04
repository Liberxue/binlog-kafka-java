package com.liberxue.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.Iterator;

class jsonObject {

    private String type;
    private String schema;
    private String table;
    private String column;
    private JSONArray row;

    //private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }
    public JSONArray getRow() {
        return row;
    }

    public void setRow(JSONArray row) {
        this.row = row;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }


}

public class Unmarshaller {

    public Unmarshaller(String jsonStr) {
        /**
         * @Title: Unmarshaller
         * @ProjectName Binlogkafka
         * @Description: TODO
         * @author liberxue
         * @date 2019/4/1211:09
         */
        JSONObject outJson = JSONObject.parseObject( jsonStr );
        System.out.print( outJson );
        String dml_data = outJson.getString( "dml_data" );
        JSONObject dml_dataObj = JSONObject.parseObject( dml_data );
        JSONArray tablesArray = dml_dataObj.getJSONArray( "tables" );
        Iterator<Object> iterator = tablesArray.iterator();// tablesArray Iterator
        while (iterator.hasNext()) {
            JSONArray objects = JSON.parseArray( tablesArray.toString() );
            Iterator<Object> tablesData = objects.iterator();//  mutations Iterator
            while (tablesData.hasNext()) {
                JSONObject tablesDataobject = (JSONObject) tablesData.next();
                String schema = tablesDataobject.getString( "schema_name" );
                String table = tablesDataobject.getString( "table_name" );
                JSONArray mutationsArray = tablesDataobject.getJSONArray( "mutations" );
                Iterator<Object> mutations = mutationsArray.iterator();//  mutations Iterator
                while (mutations.hasNext()) {
                    JSONObject arrayObj = (JSONObject) mutations.next();
                    String type = arrayObj.getString( "type" );
                    String row = arrayObj.getString( "row" );
                    JSONObject columns = JSONObject.parseObject( row );
//                                System.out.println("type>>>" + type);
//                                System.out.println("row>>>" + row);
//                                System.out.println( "columns>>>"+columns.getString("columns") );
                    // String columnsArray =columns.toJSONString("columns");
                    String columnsStr = columns.getString( "columns" );
                    JSONArray columnsArray = JSON.parseArray( columnsStr );
                    Iterator<Object> it = columnsArray.iterator();// columnsArray Iterator
                    while (it.hasNext()) {
                        // JSONObject columnsObj = (JSONObject) it.next();
                        jsonObject json = new jsonObject();
                        json.setSchema( schema );
                        json.setTable( table );
                        json.setType( type );
                        json.setRow( columnsArray );
                        String jsonString = JSON.toJSONString( json );
                        System.out.println( "-------------json -----------------------" );
                        System.out.println( jsonString );
                    }
                    System.out.println( "------------------------------------" );
                }
            }
        }
    }
}
