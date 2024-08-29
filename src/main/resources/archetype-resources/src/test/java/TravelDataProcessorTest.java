import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class TravelDataProcessorTest {

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(TravelDataProcessor.class);
    }

    @Test
    public void testProcessorWithValidInput() {
        String inputJson = "{\"data\":[{\"confort\":\"standard\",\"prix_base_per_km\":2,\"properties-client\":{\"latitude\":48.8566,\"longitude\":2.3522,\"nomClient\":\"FALL\",\"telephoneClient\":\"060786575\"},\"properties-driver\":{\"latitude\":40.4168,\"longitude\":3.7038,\"nomDriver\":\"DIOP\",\"telephoneDriver\":\"070786575\"}}]}";
        testRunner.enqueue(inputJson.getBytes());
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(TravelDataProcessor.SUCCESS);
        testRunner.getFlowFilesForRelationship(TravelDataProcessor.SUCCESS).forEach(flowFile -> {
            String result = new String(testRunner.getContentAsByteArray(flowFile));
            assertNotNull(result);
            assertTrue(result.contains("\"distance\""));
            assertTrue(result.contains("\"prix_travel\""));
        });
    }

    @Test
    public void testProcessorWithInvalidInput() {
        String inputJson = "{\"data\":[{\"confort\":\"standard\",\"prix_base_per_km\":\"incorrect_value\"}]}";
        testRunner.enqueue(inputJson.getBytes());
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(TravelDataProcessor.FAILURE);
    }

    @Test
    public void testDistanceCalculation() {
        double lat1 = 48.8566; // Paris
        double lon1 = 2.3522;
        double lat2 = 40.4168; // Madrid
        double lon2 = -3.7038;
        TravelDataProcessor processor = new TravelDataProcessor();

        double distance = processor.calculateDistance(lat1, lon1, lat2, lon2);
        assertTrue(distance > 1000000); // Check if distance is greater than 1000 km
    }
}
