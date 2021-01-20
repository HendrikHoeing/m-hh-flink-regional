package flink.utility;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class JsonGraphConverter {

    /*
    Converts input params to json object
    */
    public static JsonObject convertGraph(String title, JsonArray x, JsonArray y, String xName, String yName,
            String chartType) {

        JsonObject jsonGraph = new JsonObject();
        JsonObject layout = new JsonObject();

        layout.addProperty("title", title);     
        layout.addProperty("labelX", xName);
        layout.addProperty("labelY", yName);

        jsonGraph.add("x", x);
        jsonGraph.add("y", y);
        jsonGraph.addProperty("type", chartType);
        jsonGraph.add("layout", layout);

        return jsonGraph;
    }

    // If x and y are scalar values -> modify data afterwards
    public static JsonObject convertGraph(String title, String labelX, String labelY, String chartType) {

        JsonObject jsonGraph = new JsonObject();
        JsonObject layout = new JsonObject();

        
        layout.addProperty("title", title); 
        layout.addProperty("labelX", labelX);
        layout.addProperty("labelY", labelY);

        jsonGraph.addProperty("type", chartType);
        jsonGraph.add("layout", layout);

        return jsonGraph;
    }
}
