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
package org.neo4j.server.rest.causalclustering;

import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import javax.ws.rs.core.Response;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CoreStatusTest
{
    private CoreGraphDatabase db;
    private CausalClusteringStatus status;

    @Before
    public void setup() throws Exception
    {
        OutputFormat output = new OutputFormat( new JsonFormat(), new URI( "http://base.local:1234/" ), null );
        db = mock( CoreGraphDatabase.class );
        status = CausalClusteringStatusFactory.build( output, db );
    }

    @Test
    public void testAnswersWhenLeader()
    {
        // given
        when( db.getRole() ).thenReturn( Role.LEADER );

        // when
        Response available = status.available();
        Response readonly = status.readonly();
        Response writable = status.writable();

        // then
        assertEquals( OK.getStatusCode(), available.getStatus() );
        assertEquals( "true", available.getEntity() );

        assertEquals( NOT_FOUND.getStatusCode(), readonly.getStatus() );
        assertEquals( "false", readonly.getEntity() );

        assertEquals( OK.getStatusCode(), writable.getStatus() );
        assertEquals( "true", writable.getEntity() );
    }

    @Test
    public void testAnswersWhenCandidate()
    {
        // given
        when( db.getRole() ).thenReturn( Role.CANDIDATE );

        // when
        Response available = status.available();
        Response readonly = status.readonly();
        Response writable = status.writable();

        // then
        assertEquals( OK.getStatusCode(), available.getStatus() );
        assertEquals( "true", available.getEntity() );

        assertEquals( OK.getStatusCode(), readonly.getStatus() );
        assertEquals( "true", readonly.getEntity() );

        assertEquals( NOT_FOUND.getStatusCode(), writable.getStatus() );
        assertEquals( "false", writable.getEntity() );
    }

    @Test
    public void testAnswersWhenFollower()
    {
        // given
        when( db.getRole() ).thenReturn( Role.FOLLOWER );

        // when
        Response available = status.available();
        Response readonly = status.readonly();
        Response writable = status.writable();

        // then
        assertEquals( OK.getStatusCode(), available.getStatus() );
        assertEquals( "true", available.getEntity() );

        assertEquals( OK.getStatusCode(), readonly.getStatus() );
        assertEquals( "true", readonly.getEntity() );

        assertEquals( NOT_FOUND.getStatusCode(), writable.getStatus() );
        assertEquals( "false", writable.getEntity() );
    }
}
