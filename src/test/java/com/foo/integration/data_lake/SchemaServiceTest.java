package com.foo.integration.data_lake;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.Arrays;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.paloaltonetworks.cortex.data_lake.SchemaMetadata;
import com.paloaltonetworks.cortex.data_lake.SchemaService;
import com.paloaltonetworks.cortex.data_lake.SchemaServiceException;
import com.paloaltonetworks.cortex.data_lake.SchemaServicePayload;
import org.apache.avro.Schema;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
@FixMethodOrder(MethodSorters.JVM)
public class SchemaServiceTest {
    private final static String baseFqdn = System.getProperty("cortex.fqdn");
    private final static char[] password = "".toCharArray();
    private static Logger logger = Logger.getLogger("com.paloaltonetworks.cortex.data_lake");
    static SchemaService ss;

    @BeforeClass
    public static void init() {
        logger.setLevel(Level.WARNING);
        Handler consoleHandler = new ConsoleHandler();
        consoleHandler.setLevel(Level.FINEST);
        logger.addHandler(consoleHandler);
        try {
            String keystore = System.getProperty("cortex.clientcert");
            ss = new SchemaService(keystore, password, baseFqdn);
        } catch (IllegalArgumentException e) {
            Assume.assumeNoException("unable to get property 'cortex.clientcert'", e);
        } catch (Exception e) {
            Assume.assumeNoException(String.format("unable to run the tests due to: %s", e.getMessage()), e);
        }
    }

    @Test
    public void getAllSchemas() {
        try {
            var response = ss.get();
            var fwTraffExists = false;
            for (var item : response) {
                if (item != null && item.schemaId.equals("firewall.traffic"))
                    fwTraffExists = true;
            }
            assertTrue(fwTraffExists);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Ignore
    public void getFirewallSchema() {
        try {
            var sch = ss.get("firewall.traffic");
            assertTrue(sch.version >= 5);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void getUnknownSchema() {
        try {
            ss.get("firxxxll.traffic");
            fail("It should have failed");
        } catch (SchemaServiceException e) {
            assertTrue(e.httpStatusCode == 404);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void create() {
        try {
            var metadata = new SchemaMetadata.Builder().derived(false).operations(SchemaMetadata.Operations.ALL)
                    .partitionColumn("log_time").timestampColumns(Arrays.asList(new String[] { "log_time" }))
                    .partitionScheme(new SchemaMetadata.MetadataPartionSchema(1, SchemaMetadata.TimeUnits.YEARS))
                    .isPublic(false).streamPartitionFactor(1).build();
            var recordBuilder = SchemaServicePayload.RecordBuilder.factory("test", "xhoms", "xhoms test doc");
            recordBuilder.fieldAssembler.requiredString("xh_data").requiredString("log_time");
            var payload = SchemaServicePayload.fromRecordBuilder(recordBuilder, metadata);
            var createResponse = ss.create(payload);
            assertEquals("xhoms.test", createResponse.schemaId);
            assertTrue(createResponse.version == 1);
            var getResponse = ss.get("xhoms.test");
            assertEquals("xhoms.test", getResponse.schemaId);
            assertTrue(getResponse.version == 1);
            ss.delete("xhoms.test");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void create2() {
        try {
            var metadata = new SchemaMetadata.Builder().derived(false).operations(SchemaMetadata.Operations.ALL)
                    .partitionColumn("log_time").timestampColumns(Arrays.asList(new String[] { "log_time" }))
                    .partitionScheme(new SchemaMetadata.MetadataPartionSchema(1, SchemaMetadata.TimeUnits.YEARS))
                    .isPublic(false).streamPartitionFactor(1).build();
            final var schemaStr = "{\"type\":\"record\",\"name\":\"test\",\"namespace\":\"xhoms\",\"doc\":\"xhoms test doc\",\"fields\":[{\"name\":\"xh_data\",\"type\":\"string\"},{\"name\":\"log_time\",\"type\":\"string\"}]}";
            var payload = new SchemaServicePayload.Builder().schemaId("xhoms.test").metadata(metadata)
                    .structure(schemaStr).build();
            var createResponse = ss.create(payload);
            assertEquals("xhoms.test", createResponse.schemaId);
            assertTrue(createResponse.version == 1);
            var getResponse = ss.get("xhoms.test");
            assertEquals("xhoms.test", getResponse.schemaId);
            assertTrue(getResponse.version == 1);
            ss.delete("xhoms.test");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void create3() {
        try {
            var metadata = new SchemaMetadata.Builder().derived(false).operations(SchemaMetadata.Operations.ALL)
                    .partitionColumn("log_time").timestampColumns(Arrays.asList(new String[] { "log_time" }))
                    .partitionScheme(new SchemaMetadata.MetadataPartionSchema(1, SchemaMetadata.TimeUnits.YEARS))
                    .isPublic(false).streamPartitionFactor(1).build();
            final var testSchema = new Schema.Parser().parse(
                    "{\"type\":\"record\",\"name\":\"test\",\"namespace\":\"xhoms\",\"doc\":\"xhoms test doc\",\"fields\":[{\"name\":\"xh_data\",\"type\":\"string\"},{\"name\":\"log_time\",\"type\":\"string\"}]}");
            var payload = new SchemaServicePayload.Builder().schemaId("xhoms.test").metadata(metadata)
                    .structure(testSchema).build();
            var createResponse = ss.create(payload);
            assertEquals("xhoms.test", createResponse.schemaId);
            assertTrue(createResponse.version == 1);
            var getResponse = ss.get("xhoms.test");
            assertEquals("xhoms.test", getResponse.schemaId);
            assertTrue(getResponse.version == 1);
            ss.delete("xhoms.test");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void createDuplicated() {
        try {
            var metadata = new SchemaMetadata.Builder().derived(false).operations(SchemaMetadata.Operations.ALL)
                    .partitionColumn("log_time").timestampColumns(Arrays.asList(new String[] { "log_time" }))
                    .partitionScheme(new SchemaMetadata.MetadataPartionSchema(1, SchemaMetadata.TimeUnits.YEARS))
                    .isPublic(false).streamPartitionFactor(1).build();
            var recordBuilder = SchemaServicePayload.RecordBuilder.factory("test", "xhoms", "xhoms test doc");
            recordBuilder.fieldAssembler.requiredString("xh_data").requiredString("log_time");
            var payload = SchemaServicePayload.fromRecordBuilder(recordBuilder, metadata);
            var createResponse = ss.create(payload);
            assertEquals("xhoms.test", createResponse.schemaId);
            assertTrue(createResponse.version == 1);
            try {
                ss.create(payload);
                fail("it should have failed");
            } catch (SchemaServiceException e) {
                assertTrue(e.httpStatusCode == 409);
                ss.delete("xhoms.test");
            } catch (Exception e) {
                fail(e.getMessage());
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void update() {
        try {
            var metadata = new SchemaMetadata.Builder().derived(false).operations(SchemaMetadata.Operations.ALL)
                    .partitionColumn("log_time").timestampColumns(Arrays.asList(new String[] { "log_time" }))
                    .partitionScheme(new SchemaMetadata.MetadataPartionSchema(1, SchemaMetadata.TimeUnits.YEARS))
                    .isPublic(false).streamPartitionFactor(1).build();
            var recordBuilder = SchemaServicePayload.RecordBuilder.factory("test", "xhoms", "xhoms test doc");
            recordBuilder.fieldAssembler.requiredString("xh_data").requiredString("log_time");
            var kk2 = SchemaServicePayload.fromRecordBuilder(recordBuilder, metadata);
            var createResponse = ss.create(kk2);
            assertEquals("xhoms.test", createResponse.schemaId);
            assertEquals(Integer.valueOf(1), createResponse.version);
            ss.update(kk2);
            var getResponse = ss.get("xhoms.test");
            assertEquals("xhoms.test", getResponse.schemaId);
            assertEquals(Integer.valueOf(2), getResponse.version);
            ss.delete("xhoms.test");
            assertTrue(true);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void delete() {
        try {
            ss.delete("xhoms.random");
            fail("it should have failed");
        } catch (SchemaServiceException e) {
            assertTrue(e.httpStatusCode == 500);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}