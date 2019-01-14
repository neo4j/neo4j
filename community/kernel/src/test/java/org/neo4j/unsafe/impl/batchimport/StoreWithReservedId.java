/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class StoreWithReservedId
{
    private StoreWithReservedId()
    {
    }

    /**
     * Create new {@link NodeStore} mock with {@link RecordCursor} that returns record with
     * reserved id - {@link IdGeneratorImpl#INTEGER_MINUS_ONE}.
     *
     * @param highId the highId for the store mock
     * @return new {@link NodeStore} mock
     */
    public static NodeStore newNodeStoreMock( long highId )
    {
        return newStoreMock( NodeStore.class, new NodeRecord( -1 ), highId );
    }

    /**
     * Create new {@link RelationshipStore} mock with {@link RecordCursor} that returns record with
     * reserved id - {@link IdGeneratorImpl#INTEGER_MINUS_ONE}.
     *
     * @param highId the highId for the store mock
     * @return new {@link RelationshipStore} mock
     */
    public static RelationshipStore newRelationshipStoreMock( long highId )
    {
        return newStoreMock( RelationshipStore.class, new RelationshipRecord( -1 ), highId );
    }

    private static <R extends AbstractBaseRecord, S extends RecordStore<R>> S newStoreMock( Class<S> storeClass,
            R record, long highId )
    {
        S store = mock( storeClass );
        when( store.getHighId() ).thenReturn( highId );

        when( store.newRecord() ).thenReturn( record );

        RecordCursor<R> cursor = newReservedIdReturningRecordCursor( highId, record );
        when( store.newRecordCursor( any() ) ).thenReturn( cursor );

        return store;
    }

    @SuppressWarnings( "unchecked" )
    private static <R extends AbstractBaseRecord> RecordCursor<R> newReservedIdReturningRecordCursor( long highId,
            R record )
    {
        RecordCursor<R> cursor = mock( RecordCursor.class );
        when( cursor.next( anyInt() ) ).thenAnswer( invocation ->
        {
            long id = invocation.getArgument( 0 );
            long realId = (id == highId - 1) ? IdGeneratorImpl.INTEGER_MINUS_ONE : id;
            record.setId( realId );
            return true;
        } );
        when( cursor.acquire( anyLong(), any() ) ).thenReturn( cursor );
        return cursor;
    }
}
