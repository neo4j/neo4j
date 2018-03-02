/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema.fusion;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceCollections;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQuery.GeometryRangePredicate;
import org.neo4j.internal.kernel.api.IndexQuery.NumberRangePredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringContainsPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringPrefixPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringRangePredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringSuffixPredicate;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.array;

public class FusionIndexReaderTest
{
    private IndexReader stringReader;
    private IndexReader numberReader;
    private IndexReader spatialReader;
    private IndexReader luceneReader;
    private IndexReader temporalReader;
    private IndexReader[] allReaders;
    private FusionIndexReader fusionIndexReader;
    private static final int PROP_KEY = 1;
    private static final int LABEL_KEY = 11;

    @Before
    public void setup()
    {
        stringReader = mock( IndexReader.class );
        numberReader = mock( IndexReader.class );
        spatialReader = mock( IndexReader.class );
        temporalReader = mock( IndexReader.class );
        luceneReader = mock( IndexReader.class );
        allReaders = array( stringReader, numberReader, spatialReader, temporalReader, luceneReader );
        fusionIndexReader = new FusionIndexReader( allReaders, new FusionSelector(), SchemaIndexDescriptorFactory.forLabel( LABEL_KEY, PROP_KEY ) );
    }

    /* close */

    @Test
    public void closeMustCloseBothNativeAndLucene()
    {
        // when
        fusionIndexReader.close();

        // then
        for ( IndexReader reader : allReaders )
        {
            verify( reader, times( 1 ) ).close();
        }
    }

    // close iterator

    @Test
    public void closeIteratorMustCloseAll() throws Exception
    {
        // given
        PrimitiveLongResourceIterator[] iterators = new PrimitiveLongResourceIterator[allReaders.length];
        for ( int i = 0; i < allReaders.length; i++ )
        {
            PrimitiveLongResourceIterator iterator = mock( PrimitiveLongResourceIterator.class );
            when( allReaders[i].query( any( IndexQuery.class ) ) ).thenReturn( iterator );
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

    private PrimitiveLongResourceIterator mockReaderForQuery( IndexReader reader ) throws IndexNotApplicableKernelException
    {
        PrimitiveLongResourceIterator mockIterator = mock( PrimitiveLongResourceIterator.class );
        when( reader.query( any( IndexQuery.class ) ) ).thenReturn( mockIterator );
        return mockIterator;
    }

    /* countIndexedNodes */

    @Test
    public void countIndexedNodesMustSelectCorrectReader()
    {
        // given
        Value[][] values = FusionIndexTestHelp.valuesByGroup();
        Value[] allValues = FusionIndexTestHelp.allValues();

        for ( int i = 0; i < allReaders.length; i++ )
        {
            for ( Value value : values[i] )
            {
                verifyCountIndexedNodesWithCorrectReader( allReaders[i], value );
            }
        }

        // When passing composite keys, they are only handled by lucene
        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyCountIndexedNodesWithCorrectReader( luceneReader, firstValue, secondValue );
            }
        }
    }

    private void verifyCountIndexedNodesWithCorrectReader( IndexReader correct, Value... nativeValue )
    {
        fusionIndexReader.countIndexedNodes( 0, nativeValue );
        verify( correct, times( 1 ) ).countIndexedNodes( 0, nativeValue );
        for ( IndexReader reader : allReaders )
        {
            if ( reader != correct )
            {
                verify( reader, times( 0 ) ).countIndexedNodes( 0, nativeValue );
            }
        }
    }

    /* query */

    @Test
    public void mustSelectLuceneForCompositePredicate() throws Exception
    {
        // then
        verifyQueryWithCorrectReader( luceneReader, any( IndexQuery.class ), any( IndexQuery.class ) );
    }

    @Test
    public void mustSelectStringForExactPredicateWithNumberValue() throws Exception
    {
        // given
        for ( Object value : FusionIndexTestHelp.valuesSupportedByString() )
        {
            IndexQuery indexQuery = IndexQuery.exact( PROP_KEY, value );

            // then
            verifyQueryWithCorrectReader( stringReader, indexQuery );
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
            verifyQueryWithCorrectReader( numberReader, indexQuery );
        }
    }

    @Test
    public void mustSelectSpatialForExactPredicateWithSpatialValue() throws Exception
    {
        // given
        for ( Object value : FusionIndexTestHelp.valuesSupportedBySpatial() )
        {
            IndexQuery indexQuery = IndexQuery.exact( PROP_KEY, value );

            // then
            verifyQueryWithCorrectReader( spatialReader, indexQuery );
        }
    }

    @Test
    public void mustSelectTemporalForExactPredicateWithTemporalValue() throws Exception
    {
        // given
        for ( Object temporalValue : FusionIndexTestHelp.valuesSupportedByTemporal() )
        {
            IndexQuery indexQuery = IndexQuery.exact( PROP_KEY, temporalValue );

            // then
            verifyQueryWithCorrectReader( temporalReader, indexQuery );
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
            verifyQueryWithCorrectReader( luceneReader, indexQuery );
        }
    }

    @Test
    public void mustSelectStringForRangeStringPredicate() throws Exception
    {
        // given
        StringRangePredicate stringRange = IndexQuery.range( PROP_KEY, "abc", true, "def", false );

        // then
        verifyQueryWithCorrectReader( stringReader, stringRange );
    }

    @Test
    public void mustSelectNumberForRangeNumericPredicate() throws Exception
    {
        // given
        NumberRangePredicate numberRange = IndexQuery.range( PROP_KEY, 0, true, 1, false );

        // then
        verifyQueryWithCorrectReader( numberReader, numberRange );
    }

    @Test
    public void mustSelectSpatialForRangeGeometricPredicate() throws Exception
    {
        // given
        PointValue from = Values.pointValue( CoordinateReferenceSystem.Cartesian, 1.0, 1.0);
        PointValue to = Values.pointValue( CoordinateReferenceSystem.Cartesian, 2.0, 2.0);
        GeometryRangePredicate geometryRange = IndexQuery.range( PROP_KEY, from, true, to, false );

        // then
        verifyQueryWithCorrectReader( spatialReader, geometryRange );
    }

    @Test
    public void mustSelectStringForStringPrefixPredicate() throws Exception
    {
        // given
        StringPrefixPredicate stringPrefix = IndexQuery.stringPrefix( PROP_KEY, "abc" );

        // then
        verifyQueryWithCorrectReader( stringReader, stringPrefix );
    }

    @Test
    public void mustSelectStringForStringSuffixPredicate() throws Exception
    {
        // given
        StringSuffixPredicate stringPrefix = IndexQuery.stringSuffix( PROP_KEY, "abc" );

        // then
        verifyQueryWithCorrectReader( stringReader, stringPrefix );
    }

    @Test
    public void mustSelectStringForStringContainsPredicate() throws Exception
    {
        // given
        StringContainsPredicate stringContains = IndexQuery.stringContains( PROP_KEY, "abc" );

        // then
        verifyQueryWithCorrectReader( stringReader, stringContains );
    }

    @Test
    public void mustCombineResultFromExistsPredicate() throws Exception
    {
        // given
        IndexQuery.ExistsPredicate exists = IndexQuery.exists( PROP_KEY );
        long lastId = 0;
        for ( int i = 0; i < allReaders.length; i++ )
        {
            when( allReaders[i].query( exists ) ).thenReturn( PrimitiveLongResourceCollections.iterator( null, lastId++, lastId++ ) );
        }

        // when
        PrimitiveLongIterator result = fusionIndexReader.query( exists );

        // then
        PrimitiveLongSet resultSet = PrimitiveLongCollections.asSet( result );
        for ( long i = 0L; i < lastId; i++ )
        {
            assertTrue( "Expected to contain " + i + ", but was " + resultSet, resultSet.contains( i ) );
        }
    }

    private void verifyQueryWithCorrectReader( IndexReader expectedReader, IndexQuery... indexQuery )
            throws IndexNotApplicableKernelException
    {
        // when
        fusionIndexReader.query( indexQuery );

        // then
        verify( expectedReader, times( 1 ) ).query( indexQuery );
        for ( IndexReader reader : allReaders )
        {
            if ( reader != expectedReader )
            {
                verifyNoMoreInteractions( reader );
            }
        }
    }
}
