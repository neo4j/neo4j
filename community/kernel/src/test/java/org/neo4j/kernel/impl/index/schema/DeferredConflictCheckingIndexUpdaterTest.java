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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.UpdateMode;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.iterator;
import static org.neo4j.kernel.api.index.IndexEntryUpdate.add;
import static org.neo4j.kernel.api.index.IndexEntryUpdate.change;
import static org.neo4j.kernel.api.index.IndexEntryUpdate.remove;

public class DeferredConflictCheckingIndexUpdaterTest
{
    private final int labelId = 1;
    private final int[] propertyKeyIds = {2, 3};
    private final SchemaIndexDescriptor descriptor = SchemaIndexDescriptorFactory.forLabel( labelId, propertyKeyIds );

    @Test
    public void shouldQueryAboutAddedAndChangedValueTuples() throws Exception
    {
        // given
        IndexUpdater actual = mock( IndexUpdater.class );
        IndexReader reader = mock( IndexReader.class );
        when( reader.query( anyVararg() ) ).thenAnswer( invocation -> iterator( 0 ) );
        long nodeId = 0;
        List<IndexEntryUpdate<SchemaIndexDescriptor>> updates = new ArrayList<>();
        updates.add( add( nodeId++, descriptor, tuple( 10, 11 ) ) );
        updates.add( change( nodeId++, descriptor, tuple( "abc", "def" ), tuple( "ghi", "klm" ) ) );
        updates.add( remove( nodeId++, descriptor, tuple( 1001L, 1002L ) ) );
        updates.add( change( nodeId++, descriptor, tuple( (byte) 2, (byte) 3 ), tuple( (byte) 4, (byte) 5 ) ) );
        updates.add( add( nodeId++, descriptor, tuple( 5, "5" ) ) );
        try ( DeferredConflictCheckingIndexUpdater updater = new DeferredConflictCheckingIndexUpdater( actual, () -> reader, descriptor ) )
        {
            // when
            for ( IndexEntryUpdate<SchemaIndexDescriptor> update : updates )
            {
                updater.process( update );
                verify( actual ).process( update );
            }
        }

        // then
        for ( IndexEntryUpdate<SchemaIndexDescriptor> update : updates )
        {
            if ( update.updateMode() == UpdateMode.ADDED || update.updateMode() == UpdateMode.CHANGED )
            {
                Value[] tuple = update.values();
                IndexQuery[] query = new IndexQuery[tuple.length];
                for ( int i = 0; i < tuple.length; i++ )
                {
                    query[i] = IndexQuery.exact( propertyKeyIds[i], tuple[i] );
                }
                verify( reader ).query( query );
            }
        }
        verify( reader ).close();
        verifyNoMoreInteractions( reader );
    }

    @Test
    public void shouldThrowOnIndexEntryConflict() throws Exception
    {
        // given
        IndexUpdater actual = mock( IndexUpdater.class );
        IndexReader reader = mock( IndexReader.class );
        when( reader.query( anyVararg() ) ).thenAnswer( invocation -> iterator( 101, 202 ) );
        DeferredConflictCheckingIndexUpdater updater = new DeferredConflictCheckingIndexUpdater( actual, () -> reader, descriptor );

        // when
        updater.process( add( 0, descriptor, tuple( 10, 11 ) ) );
        try
        {
            updater.close();
            fail( "Should have failed" );
        }
        catch ( IndexEntryConflictException e )
        {
            // then good
            assertThat( e.getMessage(), containsString( "101" ) );
            assertThat( e.getMessage(), containsString( "202" ) );
        }
    }

    private Value[] tuple( Object... values )
    {
        Value[] result = new Value[values.length];
        for ( int i = 0; i < values.length; i++ )
        {
            result[i] = Values.of( values[i] );
        }
        return result;
    }
}
