package flink.utility;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class JsonGraphConverter {

    /*
    Converts input params to json object
    */
    public static JsonObject convertGraph(String title, JsonArray x, JsonArray y, String xName, String yName,
            String chartType, JsonObject layout) {

        JsonObject jsonGraph = new JsonObject();

        jsonGraph.addProperty("title", title);     
        jsonGraph.addProperty("labelX", xName);
        jsonGraph.addProperty("labelY", yName);
        jsonGraph.addProperty("type", chartType);
        jsonGraph.add("layout", layout);

        jsonGraph.add("x", x);
        jsonGraph.add("y", y);

        return jsonGraph;
    }

    // If x and y are scalar values -> modify data afterwards
    public static JsonObject convertGraph(String title, String xName, String yName, String chartType,
            JsonObject layout) {

        JsonObject jsonGraph = new JsonObject();

        jsonGraph.addProperty("title", title); 
        jsonGraph.addProperty("xname", xName);
        jsonGraph.addProperty("yname", yName);
        jsonGraph.addProperty("type", chartType);
        jsonGraph.add("layout", layout);

        return jsonGraph;
    }
}
