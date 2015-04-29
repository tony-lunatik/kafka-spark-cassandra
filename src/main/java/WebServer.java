import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Properties;

public class WebServer {

    public static void main(String[] args) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(8899), 0);
        server.createContext("/", new RequestHandler());
        server.setExecutor(null); // creates a default executor
        server.start();
    }

    static class RequestHandler implements HttpHandler {

        private final Producer<String, String> producer;

        private String kafkaTopic;

        public RequestHandler() throws IOException {

            Properties kafkaProperties = new Properties();
            kafkaProperties.load(new FileInputStream("kafka.properties"));

            Properties props = new Properties();
            props.put("metadata.broker.list", kafkaProperties.getProperty("brokers"));
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            props.put("request.required.acks", "1");

            ProducerConfig config = new ProducerConfig(props);
            producer = new Producer<>(config);

            kafkaTopic = kafkaProperties.getProperty("topic");
        }

        public void handle(HttpExchange he) throws IOException {

            final String method = he.getRequestMethod();
            final String address = he.getRemoteAddress().getAddress().toString();

            String body = "";

            if ("POST".equals(method)) {
                final InputStream is = he.getRequestBody();
                body = IOUtils.toString(is, "UTF-8");
            }

            final KeyedMessage<String, String> data = new KeyedMessage<>(kafkaTopic, address, address + " " + method + " " + body);

            producer.send(data);

            he.getResponseBody().close();
        }
    }

}
