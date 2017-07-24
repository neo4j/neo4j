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
package org.neo4j.kernel.impl.index.schema.combined;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.IndexQuery.NumberRangePredicate;
import org.neo4j.kernel.api.schema.IndexQuery.StringContainsPredicate;
import org.neo4j.kernel.api.schema.IndexQuery.StringPrefixPredicate;
import org.neo4j.kernel.api.schema.IndexQuery.StringRangePredicate;
import org.neo4j.kernel.api.schema.IndexQuery.StringSuffixPredicate;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.Value;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class CombinedIndexReaderTest
{
    private IndexReader boostReader;
    private IndexReader fallbackReader;
    private CombinedIndexReader combinedIndexReader;
    private static final int PROP_KEY = 1;

    @Before
    public void setup()
    {
        boostReader = mock( IndexReader.class );
        fallbackReader = mock( IndexReader.class );
        combinedIndexReader = new CombinedIndexReader( boostReader, fallbackReader );
    }

    /* close */

    @Test
    public void closeMustCloseBothBoostAndFallback() throws Exception
    {
        // when
        combinedIndexReader.close();

        // then
        verify( boostReader, times( 1 ) ).close();
        verify( fallbackReader, times( 1 ) ).close();
    }

    /* countIndexedNodes */

    @Test
    public void countIndexedNodesMustSelectCorrectReader() throws Exception
    {
        // given
        Value[] boostValues = CombinedIndexTestHelp.valuesSupportedByBoost();
        Value[] otherValues = CombinedIndexTestHelp.valuesNotSupportedByBoost();
        Value[] allValues = CombinedIndexTestHelp.allValues();

        // when
        for ( Value boostValue : boostValues )
        {
            verifyCountIndexedNodesWithCorrectReader( boostReader, fallbackReader, boostValue );
        }

        for ( Value otherValue : otherValues )
        {
            verifyCountIndexedNodesWithCorrectReader( fallbackReader, boostReader, otherValue );
        }

        for ( Value firstValue : allValues )
        {
            for ( Value secondValue : allValues )
            {
                verifyCountIndexedNodesWithCorrectReader( fallbackReader, boostReader, firstValue, secondValue );
            }
        }
    }

    private void verifyCountIndexedNodesWithCorrectReader( IndexReader correct, IndexReader wrong, Value... boostValue )
    {
        combinedIndexReader.countIndexedNodes( 0, boostValue );
        verify( correct, times( 1 ) ).countIndexedNodes( 0, boostValue );
        verify( wrong, times( 0 ) ).countIndexedNodes( 0, boostValue );
    }

    /* query */

    @Test
    public void mustSelectFallbackForCompositePredicate() throws Exception
    {
        // then
        verifyQueryWithCorrectReader( fallbackReader, boostReader, any( IndexQuery.class ), any( IndexQuery.class ) );
    }

    @Test
    public void mustSelectBoostForExactPredicateWithNumberValue() throws Exception
    {
        // given
        for ( Object numberValue : CombinedIndexTestHelp.valuesSupportedByBoost() )
        {
            IndexQuery indexQuery = IndexQuery.exact( PROP_KEY, numberValue );

            // then
            verifyQueryWithCorrectReader( boostReader, fallbackReader, indexQuery );
        }
    }

    @Test
    public void mustSelectFallbackForExactPredicateWithNonNumberValue() throws Exception
    {
        // given
        for ( Object nonNumberValue : CombinedIndexTestHelp.valuesNotSupportedByBoost() )
        {
            IndexQuery indexQuery = IndexQuery.exact( PROP_KEY, nonNumberValue );

            // then
            verifyQueryWithCorrectReader( fallbackReader, boostReader, indexQuery );
        }
    }

    @Test
    public void mustSelectFallbackForRangeStringPredicate() throws Exception
    {
        // given
        StringRangePredicate stringRange = IndexQuery.range( PROP_KEY, "abc", true, "def", false );

        // then
        verifyQueryWithCorrectReader( fallbackReader, boostReader, stringRange );
    }

    @Test
    public void mustSelectBoostForRangeNumericPredicate() throws Exception
    {
        // given
        NumberRangePredicate numberRange = IndexQuery.range( PROP_KEY, 0, true, 1, false );

        // then
        verifyQueryWithCorrectReader( boostReader, fallbackReader, numberRange );
    }

    @Test
    public void mustSelectFallbackForStringPrefixPredicate() throws Exception
    {
        // given
        StringPrefixPredicate stringPrefix = IndexQuery.stringPrefix( PROP_KEY, "abc" );

        // then
        verifyQueryWithCorrectReader( fallbackReader, boostReader, stringPrefix );
    }

    @Test
    public void mustSelectFallbackForStringSuffixPredicate() throws Exception
    {
        // given
        StringSuffixPredicate stringPrefix = IndexQuery.stringSuffix( PROP_KEY, "abc" );

        // then
        verifyQueryWithCorrectReader( fallbackReader, boostReader, stringPrefix );
    }

    @Test
    public void mustSelectFallbackForStringContainsPredicate() throws Exception
    {
        // given
        StringContainsPredicate stringContains = IndexQuery.stringContains( PROP_KEY, "abc" );

        // then
        verifyQueryWithCorrectReader( fallbackReader, boostReader, stringContains );
    }

    @Test
    public void mustCombineResultFromExistsPredicate() throws Exception
    {
        // given
        IndexQuery.ExistsPredicate exists = IndexQuery.exists( PROP_KEY );
        when( boostReader.query( exists ) ).thenReturn( Primitive.iterator( 0L, 1L, 3L, 4L, 7L ) );
        when( fallbackReader.query( exists ) ).thenReturn( Primitive.iterator( 2L, 5L, 6L ) );

        // when
        PrimitiveLongIterator result = combinedIndexReader.query( exists );

        // then
        PrimitiveLongSet resultSet = PrimitiveLongCollections.asSet( result );
        for ( long i = 0L; i < 8L; i++ )
        {
            assertTrue( "Expected to contain " + i + ", but was " + resultSet, resultSet.contains( i ) );
        }
    }

    private void verifyQueryWithCorrectReader( IndexReader expectedReader, IndexReader unexpectedReader, IndexQuery... indexQuery )
            throws IndexNotApplicableKernelException
    {
        // when
        combinedIndexReader.query( indexQuery );

        // then
        verify( expectedReader, times( 1 ) ).query( indexQuery );
        verifyNoMoreInteractions( unexpectedReader );
    }
}
