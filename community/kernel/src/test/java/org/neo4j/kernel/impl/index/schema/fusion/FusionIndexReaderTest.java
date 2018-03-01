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
import org.neo4j.internal.kernel.api.IndexQuery.NumberRangePredicate;
import org.neo4j.internal.kernel.api.IndexQuery.GeometryRangePredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringContainsPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringPrefixPredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringRangePredicate;
import org.neo4j.internal.kernel.api.IndexQuery.StringSuffixPredicate;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
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

public class FusionIndexReaderTest
{
    private IndexReader nativeReader;
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
        nativeReader = mock( IndexReader.class );
        spatialReader = mock( IndexReader.class );
        temporalReader = mock( IndexReader.class );
        luceneReader = mock( IndexReader.class );
        allReaders = new IndexReader[]{nativeReader, spatialReader, temporalReader, luceneReader};
        fusionIndexReader = new FusionIndexReader( nativeReader, spatialReader, temporalReader, luceneReader, new FusionSelector(),
                IndexDescriptorFactory.forLabel( LABEL_KEY, PROP_KEY ) );
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
    public void closeIteratorMustCloseNativeAndLucene() throws Exception
    {
        // given
        PrimitiveLongResourceIterator nativeIter = mock( PrimitiveLongResourceIterator.class );
        PrimitiveLongResourceIterator spatialIter = mock( PrimitiveLongResourceIterator.class );
        PrimitiveLongResourceIterator temporalIter = mock( PrimitiveLongResourceIterator.class );
        PrimitiveLongResourceIterator luceneIter = mock( PrimitiveLongResourceIterator.class );
        when( nativeReader.query( any( IndexQuery.class ) ) ).thenReturn( nativeIter );
        when( spatialReader.query( any( IndexQuery.class ) ) ).thenReturn( spatialIter );
        when( temporalReader.query( any( IndexQuery.class ) ) ).thenReturn( temporalIter );
        when( luceneReader.query( any( IndexQuery.class ) ) ).thenReturn( luceneIter );

        // when
        fusionIndexReader.query( IndexQuery.exists( PROP_KEY ) ).close();

        // then
        verify( nativeIter, times( 1 ) ).close();
        verify( spatialIter, times( 1 ) ).close();
        verify( temporalIter, times( 1 ) ).close();
        verify( luceneIter, times( 1 ) ).close();
    }

    /* countIndexedNodes */

    @Test
    public void countIndexedNodesMustSelectCorrectReader()
    {
        // given
        Value[] nativeValues = FusionIndexTestHelp.valuesSupportedByNative();
        Value[] spatialValues = FusionIndexTestHelp.valuesSupportedBySpatial();
        Value[] temporalValues = FusionIndexTestHelp.valuesSupportedByTemporal();
        Value[] otherValues = FusionIndexTestHelp.valuesNotSupportedByNativeOrSpatial();
        Value[] allValues = FusionIndexTestHelp.allValues();

        // when passing native values
        for ( Value nativeValue : nativeValues )
        {
            verifyCountIndexedNodesWithCorrectReader( nativeReader, nativeValue );
        }

        // when passing spatial values
        for ( Value spatialValue : spatialValues )
        {
            verifyCountIndexedNodesWithCorrectReader( spatialReader, spatialValue );
        }

        // when passing temporal values
        for ( Value temporalValue : temporalValues )
        {
            verifyCountIndexedNodesWithCorrectReader( temporalReader, temporalValue );
        }

        // when passing values not handled by others
        for ( Value otherValue : otherValues )
        {
            verifyCountIndexedNodesWithCorrectReader( luceneReader, otherValue );
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
    public void mustSelectNativeForExactPredicateWithNumberValue() throws Exception
    {
        // given
        for ( Object numberValue : FusionIndexTestHelp.valuesSupportedByNative() )
        {
            IndexQuery indexQuery = IndexQuery.exact( PROP_KEY, numberValue );

            // then
            verifyQueryWithCorrectReader( nativeReader, indexQuery );
        }
    }

    @Test
    public void mustSelectSpatialForExactPredicateWithSpatialValue() throws Exception
    {
        // given
        for ( Object spatialValue : FusionIndexTestHelp.valuesSupportedBySpatial() )
        {
            IndexQuery indexQuery = IndexQuery.exact( PROP_KEY, spatialValue );

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
        for ( Object nonNumberOrSpatialValue : FusionIndexTestHelp.valuesNotSupportedByNativeOrSpatial() )
        {
            IndexQuery indexQuery = IndexQuery.exact( PROP_KEY, nonNumberOrSpatialValue );

            // then
            verifyQueryWithCorrectReader( luceneReader, indexQuery );
        }
    }

    @Test
    public void mustSelectLuceneForRangeStringPredicate() throws Exception
    {
        // given
        StringRangePredicate stringRange = IndexQuery.range( PROP_KEY, "abc", true, "def", false );

        // then
        verifyQueryWithCorrectReader( luceneReader, stringRange );
    }

    @Test
    public void mustSelectNativeForRangeNumericPredicate() throws Exception
    {
        // given
        NumberRangePredicate numberRange = IndexQuery.range( PROP_KEY, 0, true, 1, false );

        // then
        verifyQueryWithCorrectReader( nativeReader, numberRange );
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
    public void mustSelectLuceneForStringPrefixPredicate() throws Exception
    {
        // given
        StringPrefixPredicate stringPrefix = IndexQuery.stringPrefix( PROP_KEY, "abc" );

        // then
        verifyQueryWithCorrectReader( luceneReader, stringPrefix );
    }

    @Test
    public void mustSelectLuceneForStringSuffixPredicate() throws Exception
    {
        // given
        StringSuffixPredicate stringPrefix = IndexQuery.stringSuffix( PROP_KEY, "abc" );

        // then
        verifyQueryWithCorrectReader( luceneReader, stringPrefix );
    }

    @Test
    public void mustSelectLuceneForStringContainsPredicate() throws Exception
    {
        // given
        StringContainsPredicate stringContains = IndexQuery.stringContains( PROP_KEY, "abc" );

        // then
        verifyQueryWithCorrectReader( luceneReader, stringContains );
    }

    @Test
    public void mustCombineResultFromExistsPredicate() throws Exception
    {
        // given
        IndexQuery.ExistsPredicate exists = IndexQuery.exists( PROP_KEY );
        when( nativeReader.query( exists ) ).thenReturn( PrimitiveLongResourceCollections.iterator( null, 0L, 1L, 4L, 7L ) );
        when( spatialReader.query( exists ) ).thenReturn( PrimitiveLongResourceCollections.iterator( null, 3L, 9L, 10L ) );
        when( temporalReader.query( exists ) ).thenReturn( PrimitiveLongResourceCollections.iterator( null, 8L, 11L, 12L ) );
        when( luceneReader.query( exists ) ).thenReturn( PrimitiveLongResourceCollections.iterator( null, 2L, 5L, 6L ) );

        // when
        PrimitiveLongIterator result = fusionIndexReader.query( exists );

        // then
        PrimitiveLongSet resultSet = PrimitiveLongCollections.asSet( result );
        for ( long i = 0L; i < 10L; i++ )
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
