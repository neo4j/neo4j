/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.core.state;

import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import org.neo4j.coreedge.discovery.ClusterTopology;
import org.neo4j.coreedge.identity.ClusterId;
import org.neo4j.logging.Log;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class BindingProcessTest
{
    private Log log = mock( Log.class );

    @Test
    public void notBootstrappableShouldDoNothingWhenNoIdPublished() throws Exception
    {
        // given
        ClusterTopology topology = new ClusterTopology( null, false, Collections.emptyMap(), Collections.emptySet() );

        BindingProcess binder = new BindingProcess( null, log );

        // when
        ClusterId boundClusterId = binder.attempt( topology );

        // then
        assertNull( boundClusterId );
    }

    @Test
    public void notBootstrappableShouldBindToDiscoveredIdWithNoLocalId() throws Exception
    {
        // given
        ClusterId commonClusterId = new ClusterId( UUID.randomUUID() );
        ClusterTopology topology = new ClusterTopology( commonClusterId, false, Collections.emptyMap(), Collections.emptySet() );

        BindingProcess binder = new BindingProcess( null, log );

        // when
        ClusterId boundClusterId = binder.attempt( topology );

        // then
        assertNotNull( boundClusterId );
        assertEquals( commonClusterId, boundClusterId );
    }

    @Test
    public void notBootstrappableShouldBindToMatchingDiscoveredId() throws Exception
    {
        // given
        ClusterId commonClusterId = new ClusterId( UUID.randomUUID() );
        ClusterId localClusterId = new ClusterId( commonClusterId.uuid() );

        ClusterTopology topology = new ClusterTopology( commonClusterId, false, Collections.emptyMap(), Collections.emptySet() );

        BindingProcess binder = new BindingProcess( localClusterId, log );

        // when
        ClusterId boundClusterId = binder.attempt( topology );

        // then
        assertNotNull( boundClusterId );
        assertEquals( commonClusterId, boundClusterId );
    }

    @Test
    public void bootstrappableShouldGenerateNewId() throws Exception
    {
        // given
        ClusterTopology topology = new ClusterTopology( null, true, Collections.emptyMap(), Collections.emptySet() );

        BindingProcess binder = new BindingProcess( null, log );

        // when
        ClusterId boundClusterId = binder.attempt( topology );

        // then
        assertNotNull( boundClusterId );
        assertNotNull( boundClusterId.uuid() );
    }

    @Test
    public void bootstrappableShouldPublishLocalId() throws Exception
    {
        // given
        ClusterId localClusterId = new ClusterId( UUID.randomUUID() );
        ClusterTopology topology = new ClusterTopology( null, true, Collections.emptyMap(), Collections.emptySet() );

        BindingProcess binder = new BindingProcess( localClusterId, log );

        // when
        ClusterId boundClusterId = binder.attempt( topology );

        // then
        assertNotNull( boundClusterId );
        assertEquals( localClusterId, boundClusterId );
    }

    @Test
    public void shouldThrowOnClusterIdMismatch() throws Exception
    {
        // given
        ClusterId localClusterId = new ClusterId( UUID.randomUUID() );
        ClusterId commonClusterId = new ClusterId( UUID.randomUUID() );
        ClusterTopology topology = new ClusterTopology( commonClusterId, false, Collections.emptyMap(), Collections.emptySet() );

        // when
        BindingProcess binder = new BindingProcess( localClusterId, log );

        // when
        try
        {
            binder.attempt( topology );
            fail();
        }
        catch ( BindingException ignored )
        {
            // then: expected
        }
    }
}
