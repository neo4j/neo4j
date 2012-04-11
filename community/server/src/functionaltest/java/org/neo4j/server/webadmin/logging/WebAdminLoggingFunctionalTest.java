package org.neo4j.server.webadmin.logging;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.NeoServer;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.RestRequest;

public class WebAdminLoggingFunctionalTest extends AbstractRestFunctionalTestBase
{
    private static FunctionalTestHelper functionalTestHelper;
    private RestRequest req;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        NeoServer server = server();
        functionalTestHelper = new FunctionalTestHelper( server );
    }

    @Before
    public void cleanTheDatabase()
    {
        cleanDatabase();
        req = RestRequest.req();
    }

    @Test
    public void givenNoServerLoggingConfigurationShouldLogAllAccessesToWebadmin()
    {
        // given
        GraphDatabaseAPI graph = server().getDatabase().graph;
        String storeDir = graph.getStoreDir();

        // when
        req.get( functionalTestHelper.webAdminUri() );
        gen.get().expectedStatus( 200 );

        // then
        File webadminHttpLogs = new File( storeDir + File.separator + "access.log" );
        assertTrue( webadminHttpLogs.exists() );

    }

    private String today()
    {
        int year = Calendar.getInstance().get( Calendar.YEAR );
        int month = Calendar.getInstance().get( Calendar.MONTH );
        int day = Calendar.getInstance().get( Calendar.DAY_OF_MONTH );

        return String.valueOf( year ) + ensureDoubleDigitString( month ) + ensureDoubleDigitString( day );

    }

    private String ensureDoubleDigitString( int number )
    {
        if ( number > 31 )
        {
            throw new RuntimeException( "No day or month can be greater than 31" );
        }

        String result = "";
        if ( number < 10 )
        {
            result = "0" + String.valueOf( number );
        }
        else
        {
            result = String.valueOf( number );
        }

        return result;
    }
}
