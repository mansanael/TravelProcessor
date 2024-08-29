import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

@Tags({"json", "nifi", "custom", "processor"})
@CapabilityDescription("Custom processor that transforms input JSON to a specified output JSON format.")
@ReadsAttributes({@ReadsAttribute(attribute="mime.type", description="Expected to be application/json")})
public class TravelDataProcessor extends AbstractProcessor {

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Successful processing.")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Failure in processing.")
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        this.descriptors = Collections.unmodifiableList(descriptors);
        this.relationships = new HashSet<>();
        this.relationships.add(SUCCESS);
        this.relationships.add(FAILURE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        FlowFile finalFlowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(InputStream in) throws IOException {
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode rootNode = mapper.readTree(in);
                        JsonNode dataNode = rootNode.path("data").get(0);

                        double basePricePerKm = dataNode.path("prix_base_per_km").asDouble();
                        JsonNode clientProperties = dataNode.path("properties-client");
                        JsonNode driverProperties = dataNode.path("properties-driver");

                        double clientLat = clientProperties.path("latitude").asDouble();
                        double clientLon = clientProperties.path("longitude").asDouble();
                        double driverLat = driverProperties.path("latitude").asDouble();
                        double driverLon = driverProperties.path("longitude").asDouble();

                        double distance = calculateDistance(clientLat, clientLon, driverLat, driverLon);

                        ObjectNode outputNode = mapper.createObjectNode();
                        outputNode.set("data", mapper.createArrayNode().add(mapper.createObjectNode()
                                .set("properties-client", clientProperties)
                                .put("distance", distance)
                                .set("properties-driver", driverProperties)
                                .put("prix_base_per_km", basePricePerKm)
                                .put("confort", dataNode.path("confort").asText())
                                .put("prix_travel", distance * basePricePerKm)));

                        mapper.writeValue(out, outputNode);
                    }
                });
            }
        });

        session.transfer(finalFlowFile, SUCCESS);
    }

    private double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
        // Using Haversine formula for calculating distance between two latitude-longitude points
        final int R = 6371; // Radius of the earth

        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c * 1000; // convert to meters

        return distance;
    }
}
