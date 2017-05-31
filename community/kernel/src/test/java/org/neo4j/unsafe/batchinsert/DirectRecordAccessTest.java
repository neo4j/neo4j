/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.unsafe.batchinsert;

import org.junit.Test;

import org.neo4j.kernel.impl.store.AbstractRecordStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.Loader;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class DirectRecordAccessTest
{
    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldClearCacheOnCommitEvenIfNoWrites() throws Exception
    {
        // GIVEN
        AbstractRecordStore<NodeRecord> store = mock( AbstractRecordStore.class );
        Loader<Long,NodeRecord,Void> loader = mock( Loader.class );
        DirectRecordAccess<Long,NodeRecord,Void> access = new DirectRecordAccess<>( store, loader );

        // WHEN loading w/o commit
        access.getOrLoad( 0L, null );
        verify( loader, times( 1 ) ).load( 0L, null );
        reset( loader );
        access.getOrLoad( 0L, null );

        // THEN records should be cached
        verifyNoMoreInteractions( loader );

        // and WHEN loading after commit
        access.commit();
        access.getOrLoad( 0L, null );

        // THEN records should be read again
        verify( loader, times( 1 ) ).load( 0L, null );
    }
}
