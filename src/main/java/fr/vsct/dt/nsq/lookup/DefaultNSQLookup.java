package fr.vsct.dt.nsq.lookup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.vsct.dt.nsq.ServerAddress;
import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Set;

public class DefaultNSQLookup implements NSQLookup {
    private final Logger LOGGER = LoggerFactory.getLogger(DefaultNSQLookup.class);

    Set<String> addresses = Sets.newHashSet();

    @Override
    public void addLookupAddress(String addr, int port) {
        if (!addr.startsWith("http")) {
            addr = "http://" + addr;
        }
        addr = addr + ":" + port;
        this.addresses.add(addr);
    }

    @Override
    public Set<ServerAddress> lookup(String topic) {
        Set<ServerAddress> addresses = Sets.newHashSet();

        for (String addr : getLookupAddresses()) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                String topicEncoded = URLEncoder.encode(topic, Charsets.UTF_8.name());
                JsonNode jsonNode = mapper.readTree(new URL(addr + "/lookup?topic=" + topicEncoded));
                LOGGER.debug("Server connection information: " + jsonNode.toString());
                JsonNode producers = jsonNode.get("data").get("producers");
                for (JsonNode node : producers) {
                    String host = node.get("broadcast_address").asText();
                    ServerAddress address = new ServerAddress(host, node.get("tcp_port").asInt());
                    addresses.add(address);
                }
            } catch (IOException e) {
                LOGGER.warn("Unable to connect to address {} for topic {}", addr, topic);
                LOGGER.debug(e.getMessage());
            }
        }
        if (addresses.isEmpty()) {
            LOGGER.warn("Unable to connect to any NSQ Lookup servers, servers tried: {} on topic {} ", this.addresses, topic);
        }
        return addresses;
    }

    public Set<String> getLookupAddresses() {
        return addresses;
    }
}
