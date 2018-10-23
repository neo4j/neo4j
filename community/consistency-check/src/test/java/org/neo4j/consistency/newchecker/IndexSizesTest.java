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
package org.neo4j.consistency.newchecker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.common.EntityType;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.index.IndexAccessor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.consistency.newchecker.ParallelExecution.DEFAULT_IDS_PER_CHUNK;
import static org.neo4j.consistency.newchecker.ParallelExecution.NOOP_EXCEPTION_HANDLER;


class IndexSizesTest
{
    private IndexSizes sizes;
    private final int highNodeId = 10000;
    private List<IndexDescriptor> indexes;

    @BeforeEach
    void setUp()
    {
        ParallelExecution execution = new ParallelExecution( 2, NOOP_EXCEPTION_HANDLER, DEFAULT_IDS_PER_CHUNK );

        indexes = new ArrayList<>();
        var indexAccessors = Mockito.mock( IndexAccessors.class );
        when( indexAccessors.onlineRules( any() ) ).thenReturn( indexes );
        when( indexAccessors.accessorFor( any() ) ).then( invocation ->
        {
            IndexAccessor mock = mock( IndexAccessor.class );
            BoundedIterable<Long> b = new BoundedIterable<>()
            {
                @Override
                public long maxCount()
                {
                    return invocation.getArgument( 0, IndexDescriptor.class ).getId(); //using id as "size"
                }

                @Override
                public void close()
                {

                }

                @Override
                public Iterator<Long> iterator()
                {
                    return Iterators.iterator();
                }
            };
            when( mock.newAllEntriesReader() ).thenReturn( b );
            return mock;
        } );

        sizes = new IndexSizes( execution, indexAccessors, highNodeId );
    }

    @Test
    void shouldSplitEvenly()
    {
        //given
        createIndexes( 3, 3 );
        //then
        assertEquals( IndexChecker.NUM_INDEXES_IN_CACHE, sizes.largeIndexes( EntityType.NODE ).size() );
        assertEquals( 6 - IndexChecker.NUM_INDEXES_IN_CACHE, sizes.smallIndexes( EntityType.NODE ).size() );
    }

    @Test
    void shouldSplitEvenlyLarge()
    {
        //given
        createIndexes( 151, 149 );
        //then
        assertEquals( 150, sizes.largeIndexes( EntityType.NODE ).size() );
        assertEquals( 150, sizes.smallIndexes( EntityType.NODE ).size() );
    }

    @Test
    void shouldHandleAllSmall()
    {
        //given
        createIndexes( 3, 0 );
        //then
        assertEquals( 3, sizes.smallIndexes( EntityType.NODE ).size() );
        assertEquals( 0, sizes.largeIndexes( EntityType.NODE ).size() );
    }

    @Test
    void shouldHandleAllLarge()
    {
        //given
        createIndexes( 0, 3 );
        //then
        assertEquals( 0, sizes.smallIndexes( EntityType.NODE ).size() );
        assertEquals( 3, sizes.largeIndexes( EntityType.NODE ).size() );
    }

    @Test
    void shouldHandleEmpty()
    {
        //then
        createIndexes( 0, 0 );
        assertEquals( 0, sizes.largeIndexes( EntityType.NODE ).size() );
        assertEquals( 0, sizes.smallIndexes( EntityType.NODE ).size() );
    }

    private void createIndexes( int numSmall, int numLarge )
    {
        SchemaDescriptor schema = mock( SchemaDescriptor.class );
        when( schema.entityType() ).thenReturn( EntityType.NODE );
        IndexPrototype.forSchema( schema ).withName( "foo" );
        IndexPrototype prototype = IndexPrototype.forSchema( schema ).withName( "foo" );

        for ( int i = 0; i < numLarge; i++ )
        {
            indexes.add( prototype.materialise( highNodeId / 2 + i ) ); //using id as "size"
        }
        for ( int i = 0; i < numSmall; i++ )
        {
            indexes.add( prototype.materialise( i ) ); //using id as "size"
        }

        try
        {
            sizes.initialize();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
}
