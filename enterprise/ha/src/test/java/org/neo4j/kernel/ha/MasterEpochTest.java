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
package org.neo4j.kernel.ha;

import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.com.RequestContext;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.com.master.ConversationManager;
import org.neo4j.kernel.ha.com.master.HandshakeResult;
import org.neo4j.kernel.ha.com.master.InvalidEpochException;
import org.neo4j.kernel.ha.com.master.MasterImpl;
import org.neo4j.kernel.ha.com.master.MasterImpl.SPI;
import org.neo4j.kernel.ha.com.master.MasterImplTest;
import org.neo4j.kernel.ha.id.IdAllocation;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.id.IdRange;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class MasterEpochTest
{
    @Test
    public void shouldFailSubsequentRequestsAfterAllocateIdsAfterMasterSwitch() throws Throwable
    {
        // GIVEN
        SPI spi = MasterImplTest.mockedSpi();
        IdAllocation servedIdAllocation = idAllocation( 0, 999 );
        when( spi.allocateIds( any( IdType.class ) ) ).thenReturn( servedIdAllocation );
        when( spi.getTransactionChecksum( anyLong() ) ).thenReturn( 10L );
        StoreId storeId = new StoreId();
        MasterImpl master = new MasterImpl( spi,
                mock( ConversationManager.class ), mock( MasterImpl.Monitor.class ),
                new Config( stringMap( ClusterSettings.server_id.name(), "1" ) ) );
        HandshakeResult handshake = master.handshake( 1, storeId ).response();
        master.start();

        // WHEN/THEN
        IdAllocation idAllocation = master.allocateIds( context( handshake.epoch() ), IdType.NODE ).response();
        assertEquals( servedIdAllocation.getHighestIdInUse(), idAllocation.getHighestIdInUse() );
        try
        {
            master.allocateIds( context( handshake.epoch()+1 ), IdType.NODE );
            fail( "Should fail with invalid epoch" );
        }
        catch ( InvalidEpochException e )
        {   // Good
        }
    }

    private IdAllocation idAllocation( long from, int length )
    {
        return new IdAllocation( new IdRange( EMPTY_LONG_ARRAY, from, length ), from+length, 0 );
    }

    private RequestContext context( long epoch )
    {
        return new RequestContext( epoch, 0, 0, 0, 0 );
    }
}
