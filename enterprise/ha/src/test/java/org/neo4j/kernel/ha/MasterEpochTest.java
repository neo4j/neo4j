/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.ha;

import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.com.RequestContext;
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
import org.neo4j.kernel.impl.store.id.IdType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.com.StoreIdTestFactory.newStoreIdForCurrentVersion;

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
        StoreId storeId = newStoreIdForCurrentVersion();
        MasterImpl master = new MasterImpl( spi,
                mock( ConversationManager.class ), mock( MasterImpl.Monitor.class ),
                Config.defaults( ClusterSettings.server_id, "1" ) );
        HandshakeResult handshake = master.handshake( 1, storeId ).response();
        master.start();

        // WHEN/THEN
        IdAllocation idAllocation = master.allocateIds( context( handshake.epoch() ), IdType.NODE ).response();
        assertEquals( servedIdAllocation.getHighestIdInUse(), idAllocation.getHighestIdInUse() );
        try
        {
            master.allocateIds( context( handshake.epoch() + 1 ), IdType.NODE );
            fail( "Should fail with invalid epoch" );
        }
        catch ( InvalidEpochException e )
        {   // Good
        }
    }

    private IdAllocation idAllocation( long from, int length )
    {
        return new IdAllocation( new IdRange( EMPTY_LONG_ARRAY, from, length ), from + length, 0 );
    }

    private RequestContext context( long epoch )
    {
        return new RequestContext( epoch, 0, 0, 0, 0 );
    }
}
