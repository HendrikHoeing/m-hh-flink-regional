package flink.utility;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigLoader {
    InputStream inputStream;

    public Properties getProperties(String file) throws Exception {
        Properties properties = new Properties();
        inputStream = getClass().getClassLoader().getResourceAsStream(file);

        if (inputStream != null) {
            properties.load(inputStream);
            return properties;
        } else {
            throw new FileNotFoundException("property file '" + file + "' not found in the classpath");
        }
    }
}
