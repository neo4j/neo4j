/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.server.rest;

import org.junit.Test;

import javax.ws.rs.core.Response;

import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MasterInfoServiceTest
{
    @Test
    public void masterShouldRespond200AndTrueWhenMaster()
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
    public void masterShouldRespond404AndFalseWhenSlave()
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
    public void masterShouldRespond404AndUNKNOWNWhenUnknown()
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
    public void slaveShouldRespond200AndTrueWhenSlave()
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
    public void slaveShouldRespond404AndFalseWhenMaster()
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
    public void slaveShouldRespond404AndUNKNOWNWhenUnknown()
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

    @Test
    public void shouldReportMasterAsGenerallyAvailableForTransactionProcessing()
    {
        // given
        HighlyAvailableGraphDatabase database = mock( HighlyAvailableGraphDatabase.class );
        when( database.role() ).thenReturn( "master" );

        MasterInfoService service = new MasterInfoService( null, database );

        // when
        Response response = service.isAvailable();

        // then
        assertEquals( 200, response.getStatus() );
        assertEquals( "master", String.valueOf( response.getEntity() ) );
    }

    @Test
    public void shouldReportSlaveAsGenerallyAvailableForTransactionProcessing()
    {
        // given
        HighlyAvailableGraphDatabase database = mock( HighlyAvailableGraphDatabase.class );
        when( database.role() ).thenReturn( "slave" );

        MasterInfoService service = new MasterInfoService( null, database );

        // when
        Response response = service.isAvailable();

        // then
        assertEquals( 200, response.getStatus() );
        assertEquals( "slave", String.valueOf( response.getEntity() ) );
    }

    @Test
    public void shouldReportNonMasterOrSlaveAsUnavailableForTransactionProcessing()
    {
        // given
        HighlyAvailableGraphDatabase database = mock( HighlyAvailableGraphDatabase.class );
        when( database.role() ).thenReturn( "unknown" );

        MasterInfoService service = new MasterInfoService( null, database );

        // when
        Response response = service.isAvailable();

        // then
        assertEquals( 404, response.getStatus() );
        assertEquals( "UNKNOWN", String.valueOf( response.getEntity() ) );
    }
}
