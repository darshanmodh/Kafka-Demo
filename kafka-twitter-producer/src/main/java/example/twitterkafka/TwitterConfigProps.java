package example.twitterkafka;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class TwitterConfigProps {

    InputStream inputStream;

    public Properties getProperties() throws IOException {
        Properties props = new Properties();
        String propsFilename=  "config.properties";
        inputStream = getClass().getClassLoader().getResourceAsStream(propsFilename);

        if(inputStream != null) {
            props.load(inputStream);
        } else {
            throw new FileNotFoundException("Property file not found.");
        }

        return props;
    }

}
