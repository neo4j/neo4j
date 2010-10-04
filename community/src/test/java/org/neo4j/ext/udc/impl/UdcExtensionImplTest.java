package org.neo4j.ext.udc.impl;

import org.apache.commons.io.FileUtils;
import org.apache.http.localserver.LocalTestServer;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static junit.framework.Assert.assertFalse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Unit testing for the UDC kernel extension.
 *
 * The UdcExtensionImpl is loaded when a new
 * GraphDatabase is instantiated, as part of
 * {@link org.neo4j.helpers.Service#load}.
 *
 */
public class UdcExtensionImplTest {

    Random rnd = new Random();

    @Before
    public void resetUdcState() {
        UdcTimerTask.successCounts.clear();
        UdcTimerTask.failureCounts.clear();
    }

    /**
     * Sanity check to make sure a database can be created
     * and destroyed.
     * @throws java.io.IOException
     */
    @Test
    public void shouldNotCrashNormalGraphdbCreation() throws IOException {
        EmbeddedGraphDatabase graphdb = createTempDatabase(null);
        destroy(graphdb);
    }

    /**
     * Expect the counts to be initialized.
     */
    @Test
    public void shouldLoadWhenNormalGraphdbIsCreated() throws IOException {
        EmbeddedGraphDatabase graphdb = createTempDatabase(null);
        // when the UDC extension successfully loads, it initializes the attempts count to 0
        Collection<Integer> successCountValues = UdcTimerTask.successCounts.values();
        assertFalse(successCountValues.isEmpty());
        Integer count = successCountValues.iterator().next();
        assertThat(count, equalTo(new Integer(0)));
        destroy(graphdb);
    }

    /**
     * Expect separate counts for each graphdb.
     */
    @Test
    public void shouldLoadForEachCreatedGraphdb() throws IOException {
        EmbeddedGraphDatabase graphdb1 = createTempDatabase(null);
        EmbeddedGraphDatabase graphdb2 = createTempDatabase(null);
        Set<String> successCountValues = UdcTimerTask.successCounts.keySet();
        assertThat(successCountValues.size(), equalTo(2));
        assertThat("this", is(not("that")));
        destroy(graphdb1);
        destroy(graphdb2);
    }

    @Test
    public void shouldRecordFailuresWhenThereIsNoServer() throws InterruptedException, IOException {
        Map<String, String> config = new HashMap<String, String>();
        config.put(UdcExtensionImpl.FIRST_DELAY_CONFIG_KEY, "100"); // first delay must be long enough to allow class initialization to complete
        config.put(UdcExtensionImpl.UDC_HOST_ADDRESS_KEY, "127.0.0.1:1"); // first delay must be long enough to allow class initialization to complete
        EmbeddedGraphDatabase graphdb = new EmbeddedGraphDatabase("should-record-failures", config);
        Thread.sleep(200);
        Collection<Integer> failureCountValues = UdcTimerTask.failureCounts.values();
        Integer count = failureCountValues.iterator().next();
        assertTrue(count > 0);
        destroy(graphdb);
    }

    @Test
    public void shouldRecordSuccessesWhenThereIsAServer() throws Exception {
        // first, set up the test server
        LocalTestServer server = new LocalTestServer(null, null);
        PingerHandler handler = new PingerHandler();
        server.register("/*", handler);
        server.start();

        final String hostname = server.getServiceHostName();
        final String serverAddress = hostname + ":" + server.getServicePort();

        Map<String, String> config = new HashMap<String, String>();
        config.put(UdcExtensionImpl.FIRST_DELAY_CONFIG_KEY, "100");
        config.put(UdcExtensionImpl.UDC_HOST_ADDRESS_KEY, serverAddress);

        EmbeddedGraphDatabase graphdb = createTempDatabase(config);
        Thread.sleep(200);
        Collection<Integer> successCountValues = UdcTimerTask.successCounts.values();
        Integer successes = successCountValues.iterator().next();
        assertTrue(successes > 0);
        Collection<Integer> failureCountValues = UdcTimerTask.failureCounts.values();
        Integer failures = failureCountValues.iterator().next();
        assertTrue(failures == 0);
        destroy(graphdb);
    }


    private EmbeddedGraphDatabase createTempDatabase(Map<String,String> config) throws IOException {
        EmbeddedGraphDatabase tempdb = null;
        String randomDbName = "tmpdb-" + rnd.nextInt();
        File possibleDirectory = new File(randomDbName);
        if (possibleDirectory.exists()) {
            FileUtils.deleteDirectory(possibleDirectory);
        }
        if (config == null) {
            tempdb = new EmbeddedGraphDatabase(randomDbName);
        } else {
            tempdb = new EmbeddedGraphDatabase(randomDbName, config);
        }
        return tempdb;
    }

    private void destroy(EmbeddedGraphDatabase dbToDestroy) throws IOException {
        dbToDestroy.shutdown();
        FileUtils.deleteDirectory(new File(dbToDestroy.getStoreDir()));
    }

}
