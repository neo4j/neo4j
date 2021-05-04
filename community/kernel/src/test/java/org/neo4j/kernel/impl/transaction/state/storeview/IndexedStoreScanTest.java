/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.api.LeaseService.NoLeaseClient;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceTypes;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptor.forAnyEntityTokens;
import static org.neo4j.kernel.impl.api.index.StoreScan.NO_EXTERNAL_UPDATES;
import static org.neo4j.kernel.impl.index.schema.TokenIndexProvider.DESCRIPTOR;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

class IndexedStoreScanTest
{
    @Test
    void shouldRunStoreScanWithinASharedLock()
    {
        var config = Config.defaults();
        var locks = mock( Locks.class );
        var index = forSchema( forAnyEntityTokens( NODE ), DESCRIPTOR ).withName( "index" ).materialise( 0 );
        var delegate = mock( StoreScan.class );
        var storeScan = new IndexedStoreScan( locks, index, config, delegate );
        var client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );

        storeScan.run( NO_EXTERNAL_UPDATES );

        var inOrder = inOrder( client, delegate );
        inOrder.verify( client ).initialize( NoLeaseClient.INSTANCE, 0, INSTANCE, config );
        inOrder.verify( client ).acquireShared( LockTracer.NONE, ResourceTypes.LABEL, index.schema().lockingKeys() );
        inOrder.verify( delegate ).run( NO_EXTERNAL_UPDATES );
        inOrder.verify( client ).close();
    }
}
