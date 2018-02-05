/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
