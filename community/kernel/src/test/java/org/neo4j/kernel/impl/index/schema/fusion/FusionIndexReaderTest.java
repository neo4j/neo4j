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
package org.neo4j.kernel.impl.index.schema.fusion;

import org.eclipse.collections.api.set.primitive.LongSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.function.Function;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.RangePredicate;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.impl.index.schema.NodeIdsIndexReaderQueryAnswer;
import org.neo4j.kernel.impl.index.schema.NodeValueIterator;
import org.neo4j.values.storable.Value;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.kernel.impl.index.schema.NodeIdsIndexReaderQueryAnswer.getIndexQueryArgument;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.fill;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.GENERIC;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.LUCENE;

abstract class FusionIndexReaderTest
{
    private static final int PROP_KEY = 1;
    private static final int LABEL_KEY = 11;
    private static final IndexDescriptor DESCRIPTOR = forSchema( forLabel( LABEL_KEY, PROP_KEY ) ).withName( "index" ).materialise( 12 );

    private final FusionVersion fusionVersion;
    private IndexReader[] aliveReaders;
    private EnumMap<IndexSlot,IndexReader> readers;
    private FusionIndexReader fusionIndexReader;

    FusionIndexReaderTest( FusionVersion fusionVersion )
    {
        this.fusionVersion = fusionVersion;
    }

    @BeforeEach
    void setup() throws IndexNotApplicableKernelException
    {
        initiateMocks();
    }

    private void initiateMocks() throws IndexNotApplicableKernelException
    {
        IndexSlot[] activeSlots = fusionVersion.aliveSlots();
        readers = new EnumMap<>( IndexSlot.class );
        fill( readers, IndexReader.EMPTY );
        aliveReaders = new IndexReader[activeSlots.length];
        for ( int i = 0; i < activeSlots.length; i++ )
        {
            IndexReader mock = mock( IndexReader.class );
            doAnswer( new NodeIdsIndexReaderQueryAnswer( DESCRIPTOR ) ).when( mock ).query( any(), any(), any(), anyBoolean(), any() );
            aliveReaders[i] = mock;
            switch ( activeSlots[i] )
            {
            case GENERIC:
                readers.put( GENERIC, mock );
                break;
            case LUCENE:
                readers.put( LUCENE, mock );
                break;
            default:
                throw new RuntimeException();
            }
        }
        fusionIndexReader = new FusionIndexReader( fusionVersion.slotSelector(), new LazyInstanceSelector<>( readers, throwingFactory() ),
                TestIndexDescriptorFactory.forLabel( LABEL_KEY, PROP_KEY ) );
    }

    private Function<IndexSlot,IndexReader> throwingFactory()
    {
        return i ->
        {
            throw new IllegalStateException( "All readers should exist already" );
        };
    }

    /* close */

    @Test
    void closeMustCloseBothNativeAndLucene()
    {
        // when
        fusionIndexReader.close();

        // then
        for ( IndexReader reader : aliveReaders )
        {
            verify( reader ).close();
        }
    }

    // close iterator

    @Test
    void closeIteratorMustCloseAll() throws Exception
    {
        // given
        IndexProgressor[] progressors = new IndexProgressor[aliveReaders.length];
        for ( int i = 0; i < aliveReaders.length; i++ )
        {
            int slot = i;
            doAnswer( invocation ->
            {
                IndexProgressor.EntityValueClient client = invocation.getArgument( 1 );
                IndexProgressor progressor = mock( IndexProgressor.class );
                client.initialize( DESCRIPTOR, progressor, getIndexQueryArgument( invocation ), invocation.getArgument( 2 ),
                        invocation.getArgument( 3 ), false );
                progressors[slot] = progressor;
                return null;
            } ).when( aliveReaders[i] ).query( any(), any(), any(), anyBoolean(), any() );
        }

        // when
        try ( NodeValueIterator iterator = new NodeValueIterator() )
        {
            fusionIndexReader.query( NULL_CONTEXT, iterator, IndexOrder.NONE, false, IndexQuery.exists( PROP_KEY ) );
        }

        // then
        for ( IndexProgressor progressor : progressors )
        {
            verify( progressor ).close();
        }
    }

    /* countIndexedNodes */

    @Test
    void countIndexedNodesMustSelectCorrectReader()
    {
        // given
        EnumMap<IndexSlot,Value[]> values = FusionIndexTestHelp.valuesByGroup();
        Value[] allValues = FusionIndexTestHelp.allValues();

        for ( IndexSlot slot : IndexSlot.values() )
        {
            for ( Value value : values.get( slot ) )
            {
                verifyCountIndexedNodesWithCorrectReader( readers.get( slot ), value );
            }
        }

        // When passing composite keys, they are only handled by generic
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyCountIndexedNodesWithCorrectReader( readers.get( GENERIC ), firstValue, secondValue );
            }
        }
    }

    private void verifyCountIndexedNodesWithCorrectReader( IndexReader correct, Value... nativeValue )
    {
        fusionIndexReader.countIndexedNodes( 0, new int[] {PROP_KEY}, nativeValue );
        verify( correct ).countIndexedNodes( 0, new int[] {PROP_KEY}, nativeValue );
        for ( IndexReader reader : aliveReaders )
        {
            if ( reader != correct )
            {
                verify( reader, never() ).countIndexedNodes( 0, new int[] {PROP_KEY}, nativeValue );
            }
        }
    }

    /* query */

    @Test
    void mustSelectLuceneForCompositePredicate() throws Exception
    {
        // then
        verifyQueryWithCorrectReader( readers.get( GENERIC ), IndexQuery.exists( 0 ), IndexQuery.exists( 1 ) );
    }

    @Test
    void mustSelectLuceneForExactPredicateWithStringValue() throws Exception
    {
        // given
        for ( Object value : FusionIndexTestHelp.valuesSupportedByLucene() )
        {
            IndexQuery indexQuery = IndexQuery.exact( PROP_KEY, value );

            // then
            verifyQueryWithCorrectReader( readers.get( LUCENE ), indexQuery );
        }
    }

    @Test
    void mustSelectGenericForExactPredicateWithOtherValue() throws Exception
    {
        // given
        for ( Object value : FusionIndexTestHelp.valuesNotSupportedBySpecificIndex() )
        {
            IndexQuery indexQuery = IndexQuery.exact( PROP_KEY, value );

            // then
            verifyQueryWithCorrectReader( readers.get( GENERIC ), indexQuery );
        }
    }

    @Test
    void mustSelectLuceneForRangeStringPredicate() throws Exception
    {
        // given
        RangePredicate<?> numberRange = IndexQuery.range( PROP_KEY, "a", true, "b", false );

        // then
        verifyQueryWithCorrectReader( readers.get( LUCENE ), numberRange );
    }

    @Test
    void mustCombineResultFromExistsPredicate() throws Exception
    {
        // given
        IndexQuery.ExistsPredicate exists = IndexQuery.exists( PROP_KEY );
        long lastId = 0;
        for ( IndexReader aliveReader : aliveReaders )
        {
            doAnswer( new NodeIdsIndexReaderQueryAnswer( DESCRIPTOR, lastId++, lastId++ ) ).when( aliveReader ).query(
                    any(), any(), any(), anyBoolean(), any() );
        }

        // when
        LongSet resultSet;
        try ( NodeValueIterator result = new NodeValueIterator() )
        {
            fusionIndexReader.query( NULL_CONTEXT, result, IndexOrder.NONE, false, exists );

            // then
            resultSet = PrimitiveLongCollections.asSet( result );
            for ( long i = 0L; i < lastId; i++ )
            {
                assertTrue( resultSet.contains( i ), "Expected to contain " + i + ", but was " + resultSet );
            }
        }
    }

    @Test
    void shouldInstantiatePartLazilyForSpecificValueGroupQuery() throws IndexNotApplicableKernelException
    {
        // given
        EnumMap<IndexSlot,Value[]> values = FusionIndexTestHelp.valuesByGroup();
        for ( IndexSlot i : IndexSlot.values() )
        {
            if ( readers.get( i ) != IndexReader.EMPTY )
            {
                // when
                Value value = values.get( i )[0];
                try ( NodeValueIterator cursor = new NodeValueIterator() )
                {
                    fusionIndexReader.query( NULL_CONTEXT, cursor, IndexOrder.NONE, false, IndexQuery.exact( 0, value ) );
                }
                for ( IndexSlot j : IndexSlot.values() )
                {
                    // then
                    if ( readers.get( j ) != IndexReader.EMPTY )
                    {
                        if ( i == j )
                        {
                            verify( readers.get( i ) ).query( any(), any(), any(), anyBoolean(), any() );
                        }
                        else
                        {
                            verifyNoMoreInteractions( readers.get( j ) );
                        }
                    }
                }
            }

            initiateMocks();
        }
    }

    private void verifyQueryWithCorrectReader( IndexReader expectedReader, IndexQuery... indexQuery )
            throws IndexNotApplicableKernelException
    {
        // when
        try ( NodeValueIterator cursor = new NodeValueIterator() )
        {
            fusionIndexReader.query( NULL_CONTEXT, cursor, IndexOrder.NONE, false, indexQuery );
        }

        // then
        // Strange mockito inconsistency regarding varargs
        if ( indexQuery.length == 1 )
        {
            verify( expectedReader ).query( any(), any(), any(), anyBoolean(), eq( indexQuery[0] ) );
        }
        else
        {
            verify( expectedReader ).query( any(), any(), any(), anyBoolean(), eq( indexQuery[0] ), eq( indexQuery[1] ) );
        }
        for ( IndexReader reader : aliveReaders )
        {
            if ( reader != expectedReader )
            {
                verifyNoMoreInteractions( reader );
            }
        }
    }
}
