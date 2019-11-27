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
package org.neo4j.kernel.impl.index.schema;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.UpdateMode;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.storageengine.api.IndexEntryUpdate.add;
import static org.neo4j.storageengine.api.IndexEntryUpdate.change;
import static org.neo4j.storageengine.api.IndexEntryUpdate.remove;

class DeferredConflictCheckingIndexUpdaterTest
{
    private static final int labelId = 1;
    private final int[] propertyKeyIds = {2, 3};
    private final IndexDescriptor descriptor = TestIndexDescriptorFactory.forLabel( labelId, propertyKeyIds );

    @Test
    void shouldQueryAboutAddedAndChangedValueTuples() throws Exception
    {
        // given
        IndexUpdater actual = mock( IndexUpdater.class );
        IndexReader reader = mock( IndexReader.class );
        doAnswer( new NodeIdsIndexReaderQueryAnswer( descriptor, 0 ) ).when( reader ).query( any(), any(), any(), anyBoolean(), any() );
        long nodeId = 0;
        List<IndexEntryUpdate<IndexDescriptor>> updates = new ArrayList<>();
        updates.add( add( nodeId++, descriptor, tuple( 10, 11 ) ) );
        updates.add( change( nodeId++, descriptor, tuple( "abc", "def" ), tuple( "ghi", "klm" ) ) );
        updates.add( remove( nodeId++, descriptor, tuple( 1001L, 1002L ) ) );
        updates.add( change( nodeId++, descriptor, tuple( (byte) 2, (byte) 3 ), tuple( (byte) 4, (byte) 5 ) ) );
        updates.add( add( nodeId, descriptor, tuple( 5, "5" ) ) );
        try ( DeferredConflictCheckingIndexUpdater updater = new DeferredConflictCheckingIndexUpdater( actual, () -> reader, descriptor ) )
        {
            // when
            for ( IndexEntryUpdate<IndexDescriptor> update : updates )
            {
                updater.process( update );
                verify( actual ).process( update );
            }
        }

        // then
        for ( IndexEntryUpdate<IndexDescriptor> update : updates )
        {
            if ( update.updateMode() == UpdateMode.ADDED || update.updateMode() == UpdateMode.CHANGED )
            {
                Value[] tuple = update.values();
                IndexQuery[] query = new IndexQuery[tuple.length];
                for ( int i = 0; i < tuple.length; i++ )
                {
                    query[i] = IndexQuery.exact( propertyKeyIds[i], tuple[i] );
                }
                verify( reader ).query( any(), any(), any(), anyBoolean(), eq( query[0] ), eq( query[1] ) );
            }
        }
        verify( reader ).close();
        verifyNoMoreInteractions( reader );
    }

    @Test
    void shouldThrowOnIndexEntryConflict() throws Exception
    {
        // given
        IndexUpdater actual = mock( IndexUpdater.class );
        IndexReader reader = mock( IndexReader.class );
        doAnswer( new NodeIdsIndexReaderQueryAnswer( descriptor, 101, 202 ) ).when( reader ).query( any(), any(), any(), anyBoolean(), any() );
        DeferredConflictCheckingIndexUpdater updater = new DeferredConflictCheckingIndexUpdater( actual, () -> reader, descriptor );

        // when
        updater.process( add( 0, descriptor, tuple( 10, 11 ) ) );
        var e = assertThrows( IndexEntryConflictException.class, updater::close );
        assertThat( e.getMessage() ).contains( "101" );
        assertThat( e.getMessage() ).contains( "202" );
    }

    private static Value[] tuple( Object... values )
    {
        Value[] result = new Value[values.length];
        for ( int i = 0; i < values.length; i++ )
        {
            result[i] = Values.of( values[i] );
        }
        return result;
    }
}
