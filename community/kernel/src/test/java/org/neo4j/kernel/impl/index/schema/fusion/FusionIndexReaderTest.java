/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.EnumMap;
import java.util.function.Function;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.collection.PrimitiveLongResourceCollections;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.RangePredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringContainsPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringPrefixPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringSuffixPredicate;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexTestHelp.fill;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionVersion.v00;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionVersion.v10;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionVersion.v20;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.LUCENE;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.NUMBER;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.SPATIAL;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.STRING;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.TEMPORAL;
import static org.neo4j.values.storable.Values.stringValue;

@RunWith( Parameterized.class )
public class FusionIndexReaderTest
{
    private IndexReader[] aliveReaders;
    private EnumMap<IndexSlot,IndexReader> readers;
    private FusionIndexReader fusionIndexReader;
    private static final int PROP_KEY = 1;
    private static final int LABEL_KEY = 11;

    @Parameterized.Parameters( name = "{0}" )
    public static FusionVersion[] versions()
    {
        return new FusionVersion[]
                {
                        v00, v10, v20
                };
    }

    @Parameterized.Parameter
    public static FusionVersion fusionVersion;

    @Before
    public void setup()
    {
        initiateMocks();
    }

    private void initiateMocks()
    {
        IndexSlot[] activeSlots = fusionVersion.aliveSlots();
        readers = new EnumMap<>( IndexSlot.class );
        fill( readers, IndexReader.EMPTY );
        aliveReaders = new IndexReader[activeSlots.length];
        for ( int i = 0; i < activeSlots.length; i++ )
        {
            IndexReader mock = mock( IndexReader.class );
            aliveReaders[i] = mock;
            switch ( activeSlots[i] )
            {
            case STRING:
                readers.put( STRING, mock );
                break;
            case NUMBER:
                readers.put( NUMBER, mock );
                break;
            case SPATIAL:
                readers.put( SPATIAL, mock );
                break;
            case TEMPORAL:
                readers.put( TEMPORAL, mock );
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
    public void closeMustCloseBothNativeAndLucene()
    {
        // when
        fusionIndexReader.close();

        // then
        for ( IndexReader reader : aliveReaders )
        {
            verify( reader, times( 1 ) ).close();
        }
    }

    // close iterator

    @Test
    public void closeIteratorMustCloseAll() throws Exception
    {
        // given
        PrimitiveLongResourceIterator[] iterators = new PrimitiveLongResourceIterator[aliveReaders.length];
        for ( int i = 0; i < aliveReaders.length; i++ )
        {
            PrimitiveLongResourceIterator iterator = mock( PrimitiveLongResourceIterator.class );
            when( aliveReaders[i].query( any( IndexQuery.class ) ) ).thenReturn( iterator );
            iterators[i] = iterator;
        }

        // when
        fusionIndexReader.query( IndexQuery.exists( PROP_KEY ) ).close();

        // then
        for ( PrimitiveLongResourceIterator iterator : iterators )
        {
            verify( iterator, times( 1 ) ).close();
        }
    }

    /* countIndexedNodes */

    @Test
    public void countIndexedNodesMustSelectCorrectReader()
    {
        // given
        EnumMap<IndexSlot,Value[]> values = FusionIndexTestHelp.valuesByGroup();
        Value[] allValues = FusionIndexTestHelp.allValues();

        for ( IndexSlot slot : IndexSlot.values() )
        {
            for ( Value value : values.get( slot ) )
            {
                verifyCountIndexedNodesWithCorrectReader( orLucene( readers.get( slot ) ), value );
            }
        }

        // When passing composite keys, they are only handled by lucene
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyCountIndexedNodesWithCorrectReader( readers.get( LUCENE ), firstValue, secondValue );
            }
        }
    }

    private void verifyCountIndexedNodesWithCorrectReader( IndexReader correct, Value... nativeValue )
    {
        fusionIndexReader.countIndexedNodes( 0, new int[] {PROP_KEY}, nativeValue );
        verify( correct, times( 1 ) ).countIndexedNodes( 0, new int[] {PROP_KEY}, nativeValue );
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
    public void mustSelectLuceneForCompositePredicate() throws Exception
    {
        // then
        verifyQueryWithCorrectReader( readers.get( LUCENE ), any( IndexQuery.class ), any( IndexQuery.class ) );
    }

    @Test
    public void mustSelectStringForExactPredicateWithNumberValue() throws Exception
    {
        // given
        for ( Object value : FusionIndexTestHelp.valuesSupportedByString() )
        {
            IndexQuery indexQuery = IndexQuery.exact( PROP_KEY, value );

            // then
            verifyQueryWithCorrectReader( expectedForStrings(), indexQuery );
        }
    }

    @Test
    public void mustSelectNumberForExactPredicateWithNumberValue() throws Exception
    {
        // given
        for ( Object value : FusionIndexTestHelp.valuesSupportedByNumber() )
        {
            IndexQuery indexQuery = IndexQuery.exact( PROP_KEY, value );

            // then
            verifyQueryWithCorrectReader( expectedForNumbers(), indexQuery );
        }
    }

    @Test
    public void mustSelectSpatialForExactPredicateWithSpatialValue() throws Exception
    {
        // given
        assumeTrue( hasSpatialSupport() );
        for ( Object value : FusionIndexTestHelp.valuesSupportedBySpatial() )
        {
            IndexQuery indexQuery = IndexQuery.exact( PROP_KEY, value );

            // then
            verifyQueryWithCorrectReader( readers.get( SPATIAL ), indexQuery );
        }
    }

    @Test
    public void mustSelectTemporalForExactPredicateWithTemporalValue() throws Exception
    {
        // given
        assumeTrue( hasTemporalSupport() );
        for ( Object temporalValue : FusionIndexTestHelp.valuesSupportedByTemporal() )
        {
            IndexQuery indexQuery = IndexQuery.exact( PROP_KEY, temporalValue );

            // then
            verifyQueryWithCorrectReader( readers.get( TEMPORAL ), indexQuery );
        }
    }

    @Test
    public void mustSelectLuceneForExactPredicateWithOtherValue() throws Exception
    {
        // given
        for ( Object value : FusionIndexTestHelp.valuesNotSupportedBySpecificIndex() )
        {
            IndexQuery indexQuery = IndexQuery.exact( PROP_KEY, value );

            // then
            verifyQueryWithCorrectReader( readers.get( LUCENE ), indexQuery );
        }
    }

    @Test
    public void mustSelectStringForRangeStringPredicate() throws Exception
    {
        // given
        RangePredicate<?> stringRange = IndexQuery.range( PROP_KEY, "abc", true, "def", false );

        // then
        verifyQueryWithCorrectReader( expectedForStrings(), stringRange );
    }

    @Test
    public void mustSelectNumberForRangeNumericPredicate() throws Exception
    {
        // given
        RangePredicate<?> numberRange = IndexQuery.range( PROP_KEY, 0, true, 1, false );

        // then
        verifyQueryWithCorrectReader( expectedForNumbers(), numberRange );
    }

    @Test
    public void mustSelectSpatialForRangeGeometricPredicate() throws Exception
    {
        // given
        assumeTrue( hasSpatialSupport() );
        PointValue from = Values.pointValue( CoordinateReferenceSystem.Cartesian, 1.0, 1.0);
        PointValue to = Values.pointValue( CoordinateReferenceSystem.Cartesian, 2.0, 2.0);
        RangePredicate<?> geometryRange = IndexQuery.range( PROP_KEY, from, true, to, false );

        // then
        verifyQueryWithCorrectReader( readers.get( SPATIAL ), geometryRange );
    }

    @Test
    public void mustSelectStringForStringPrefixPredicate() throws Exception
    {
        // given
        StringPrefixPredicate stringPrefix = IndexQuery.stringPrefix( PROP_KEY, stringValue( "abc" ) );

        // then
        verifyQueryWithCorrectReader( expectedForStrings(), stringPrefix );
    }

    @Test
    public void mustSelectStringForStringSuffixPredicate() throws Exception
    {
        // given
        StringSuffixPredicate stringPrefix = IndexQuery.stringSuffix( PROP_KEY, stringValue( "abc" ) );

        // then
        verifyQueryWithCorrectReader( expectedForStrings(), stringPrefix );
    }

    @Test
    public void mustSelectStringForStringContainsPredicate() throws Exception
    {
        // given
        StringContainsPredicate stringContains = IndexQuery.stringContains( PROP_KEY, stringValue( "abc" ) );

        // then
        verifyQueryWithCorrectReader( expectedForStrings(), stringContains );
    }

    @Test
    public void mustCombineResultFromExistsPredicate() throws Exception
    {
        // given
        IndexQuery.ExistsPredicate exists = IndexQuery.exists( PROP_KEY );
        long lastId = 0;
        for ( IndexReader aliveReader : aliveReaders )
        {
            when( aliveReader.query( exists ) ).thenReturn( PrimitiveLongResourceCollections.iterator( null, lastId++, lastId++ ) );
        }

        // when
        LongIterator result = fusionIndexReader.query( exists );

        // then

        LongSet resultSet = PrimitiveLongCollections.asSet( result );
        for ( long i = 0L; i < lastId; i++ )
        {
            assertTrue( "Expected to contain " + i + ", but was " + resultSet, resultSet.contains( i ) );
        }
    }

    @Test
    public void shouldInstantiatePartLazilyForSpecificValueGroupQuery() throws IndexNotApplicableKernelException
    {
        // given
        EnumMap<IndexSlot,Value[]> values = FusionIndexTestHelp.valuesByGroup();
        for ( IndexSlot i : IndexSlot.values() )
        {
            if ( readers.get( i ) != IndexReader.EMPTY )
            {
                // when
                Value value = values.get( i )[0];
                fusionIndexReader.query( IndexQuery.exact( 0, value ) );
                for ( IndexSlot j : IndexSlot.values() )
                {
                    // then
                    if ( readers.get( j ) != IndexReader.EMPTY )
                    {
                        if ( i == j )
                        {
                            verify( readers.get( i ) ).query( any( IndexQuery.class ) );
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
        fusionIndexReader.query( indexQuery );

        // then
        verify( expectedReader, times( 1 ) ).query( indexQuery );
        for ( IndexReader reader : aliveReaders )
        {
            if ( reader != expectedReader )
            {
                verifyNoMoreInteractions( reader );
            }
        }
    }

    private IndexReader expectedForStrings()
    {
        return orLucene( readers.get( STRING ) );
    }

    private IndexReader expectedForNumbers()
    {
        return orLucene( readers.get( NUMBER ) );
    }

    private boolean hasSpatialSupport()
    {
        return readers.get( SPATIAL ) != IndexReader.EMPTY;
    }

    private boolean hasTemporalSupport()
    {
        return readers.get( TEMPORAL ) != IndexReader.EMPTY;
    }

    private IndexReader orLucene( IndexReader reader )
    {
        return reader != IndexReader.EMPTY ? reader : readers.get( LUCENE );
    }
}
