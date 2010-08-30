package org.neo4j.examples;

import static org.junit.Assert.assertTrue;

import java.util.Date;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.management.Kernel;

public class JmxTest
{
    @Test
    public void readJmxProperties()
    {
        GraphDatabaseService graphDbService = new EmbeddedGraphDatabase(
                "target/jmx-db" );
        Date startTime = getStartTimeFromManagementBean( graphDbService );
        Date now = new Date();
        System.out.println( startTime + " " + now );
        assertTrue( startTime.before( now ) || startTime.equals( now ) );
    }

    // START SNIPPET: getStartTime
    private static Date getStartTimeFromManagementBean(
            GraphDatabaseService graphDbService )
    {
        // use EmbeddedGraphDatabase to access management beans
        EmbeddedGraphDatabase graphDb = (EmbeddedGraphDatabase) graphDbService;
        Kernel kernel = graphDb.getManagementBean( Kernel.class );
        Date startTime = kernel.getKernelStartTime();
        return startTime;
    }
    // END SNIPPET: getStartTime
}
