package org.neo4j.server.webadmin.rest;

import javax.ws.rs.core.Response;

import org.junit.Test;

import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MasterInfoServiceTest
{
    @Test
    public void masterShouldRespond200AndTrueWhenMaster() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase database = mock( HighlyAvailableGraphDatabase.class );
        when( database.role() ).thenReturn( "master" );

        MasterInfoService service = new MasterInfoService( null, database );

        // when
        Response response = service.isMaster();

        // then
        assertEquals( 200, response.getStatus() );
        assertEquals( "true", String.valueOf( response.getEntity() ) );
    }

    @Test
    public void masterShouldRespond404AndFalseWhenSlave() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase database = mock( HighlyAvailableGraphDatabase.class );
        when( database.role() ).thenReturn( "slave" );

        MasterInfoService service = new MasterInfoService( null, database );

        // when
        Response response = service.isMaster();

        // then
        assertEquals( 404, response.getStatus() );
        assertEquals( "false", String.valueOf( response.getEntity() ) );
    }

    @Test
    public void masterShouldRespond404AndUNKNOWNWhenUnknown() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase database = mock( HighlyAvailableGraphDatabase.class );
        when( database.role() ).thenReturn( "unknown" );

        MasterInfoService service = new MasterInfoService( null, database );

        // when
        Response response = service.isMaster();

        // then
        assertEquals( 404, response.getStatus() );
        assertEquals( "UNKNOWN", String.valueOf( response.getEntity() ) );
    }

    @Test
    public void slaveShouldRespond200AndTrueWhenSlave() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase database = mock( HighlyAvailableGraphDatabase.class );
        when( database.role() ).thenReturn( "slave" );

        MasterInfoService service = new MasterInfoService( null, database );

        // when
        Response response = service.isSlave();

        // then
        assertEquals( 200, response.getStatus() );
        assertEquals( "true", String.valueOf( response.getEntity() ) );
    }

    @Test
    public void slaveShouldRespond404AndFalseWhenMaster() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase database = mock( HighlyAvailableGraphDatabase.class );
        when( database.role() ).thenReturn( "master" );

        MasterInfoService service = new MasterInfoService( null, database );

        // when
        Response response = service.isSlave();

        // then
        assertEquals( 404, response.getStatus() );
        assertEquals( "false", String.valueOf( response.getEntity() ) );
    }

    @Test
    public void slaveShouldRespond404AndUNKNOWNWhenUnknown() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase database = mock( HighlyAvailableGraphDatabase.class );
        when( database.role() ).thenReturn( "unknown" );

        MasterInfoService service = new MasterInfoService( null, database );

        // when
        Response response = service.isSlave();

        // then
        assertEquals( 404, response.getStatus() );
        assertEquals( "UNKNOWN", String.valueOf( response.getEntity() ) );
    }
}
