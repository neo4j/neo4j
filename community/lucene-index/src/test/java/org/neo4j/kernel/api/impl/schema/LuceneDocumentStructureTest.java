/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.schema;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.junit.Test;

import static junit.framework.Assert.assertNull;
import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure.NODE_ID_KEY;
import static org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure.newSeekQuery;
import static org.neo4j.kernel.api.impl.schema.ValueEncoding.Array;
import static org.neo4j.kernel.api.impl.schema.ValueEncoding.Bool;
import static org.neo4j.kernel.api.impl.schema.ValueEncoding.Number;
import static org.neo4j.kernel.api.impl.schema.ValueEncoding.String;

public class LuceneDocumentStructureTest
{
    @Test
    public void tooLongStringShouldBeSkipped()
    {
        String string = RandomStringUtils.randomAscii( 358749 );
        Document document = LuceneDocumentStructure.documentRepresentingProperty( 123, string );
        assertNull( document.getField( String.key() ) );
    }

    @Test
    public void tooLongArrayShouldBeSkipped()
    {
        byte[] bytes = RandomStringUtils.randomAscii( IndexWriter.MAX_TERM_LENGTH + 10 ).getBytes();
        Document document = LuceneDocumentStructure.documentRepresentingProperty( 123, bytes );
        assertNull( document.getField( Array.key() ) );
    }

    @Test
    public void stringWithMaximumLengthShouldBeAllowed()
    {
        String longestString = RandomStringUtils.randomAscii( IndexWriter.MAX_TERM_LENGTH );
        Document document = LuceneDocumentStructure.documentRepresentingProperty( 123, longestString );
        assertEquals( longestString, document.getField( String.key() ).stringValue() );
    }

    @Test
    public void shouldBuildDocumentRepresentingStringProperty() throws Exception
    {
        // given
        Document document = LuceneDocumentStructure.documentRepresentingProperty( 123, "hello" );

        // then
        assertEquals( "123", document.get( NODE_ID_KEY ) );
        assertEquals( "hello", document.get( String.key() ) );
    }

    @Test
    public void shouldBuildDocumentRepresentingBoolProperty() throws Exception
    {
        // given
        Document document = LuceneDocumentStructure.documentRepresentingProperty( 123, true );

        // then
        assertEquals( "123", document.get( NODE_ID_KEY ) );
        assertEquals( "true", document.get( Bool.key() ) );
    }

    @Test
    public void shouldBuildDocumentRepresentingNumberProperty() throws Exception
    {
        // given
        Document document = LuceneDocumentStructure.documentRepresentingProperty( 123, 12 );

        // then
        assertEquals( "123", document.get( NODE_ID_KEY ) );
        assertEquals( 12.0, document.getField( Number.key() ).numericValue().doubleValue() );
    }

    @Test
    public void shouldBuildDocumentRepresentingArrayProperty() throws Exception
    {
        // given
        Document document = LuceneDocumentStructure.documentRepresentingProperty( 123, new Integer[]{1, 2, 3} );

        // then
        assertEquals( "123", document.get( NODE_ID_KEY ) );
        assertEquals( "D1.0|2.0|3.0|", document.get( Array.key() ) );
    }

    @Test
    public void shouldBuildQueryRepresentingBoolProperty() throws Exception
    {
        // given
        ConstantScoreQuery constantScoreQuery = (ConstantScoreQuery) newSeekQuery( true );
        TermQuery query = (TermQuery) constantScoreQuery.getQuery();

        // then
        assertEquals( "true", query.getTerm().text() );
    }

    @Test
    public void shouldBuildQueryRepresentingStringProperty() throws Exception
    {
        // given
        ConstantScoreQuery query = (ConstantScoreQuery) newSeekQuery( "Characters" );

        // then
        assertEquals( "Characters", ((TermQuery) query.getQuery()).getTerm().text() );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldBuildQueryRepresentingNumberProperty() throws Exception
    {
        // given
        ConstantScoreQuery constantScoreQuery = (ConstantScoreQuery) newSeekQuery( 12 );
        NumericRangeQuery<Double> query = (NumericRangeQuery<Double>) constantScoreQuery.getQuery();

        // then
        assertEquals( 12.0, query.getMin() );
        assertEquals( 12.0, query.getMax() );
    }

    @Test
    public void shouldBuildQueryRepresentingArrayProperty() throws Exception
    {
        // given
        ConstantScoreQuery constantScoreQuery = (ConstantScoreQuery)
                newSeekQuery( new Integer[]{1, 2, 3} );
        TermQuery query = (TermQuery) constantScoreQuery.getQuery();

        // then
        assertEquals( "D1.0|2.0|3.0|", query.getTerm().text() );
    }

    @Test
    public void shouldBuildRangeSeekByNumberQueryForStrings() throws Exception
    {
        // given
        NumericRangeQuery<Double> query = LuceneDocumentStructure.newInclusiveNumericRangeSeekQuery( 12.0d, null );

        // then
        assertEquals( "number", query.getField() );
        assertEquals( 12.0, query.getMin() );
        assertEquals( true, query.includesMin() );
        assertEquals( null, query.getMax() );
        assertEquals( true, query.includesMax() );
    }

    @Test
    public void shouldBuildRangeSeekByStringQueryForStrings() throws Exception
    {
        // given
        ConstantScoreQuery constantQuery =
                (ConstantScoreQuery) LuceneDocumentStructure.newRangeSeekByStringQuery( "foo", false, null, true );
        TermRangeQuery query = (TermRangeQuery) constantQuery.getQuery();

        // then
        assertEquals( "string", query.getField() );
        assertEquals( "foo", query.getLowerTerm().utf8ToString() );
        assertEquals( false, query.includesLower() );
        assertEquals( null, query.getUpperTerm() );
        assertEquals( true, query.includesUpper() );
    }

    @Test
    public void shouldBuildWildcardQueries() throws Exception
    {
        // given
        WildcardQuery query = (WildcardQuery) LuceneDocumentStructure.newWildCardStringQuery( "foo" );

        // then
        assertEquals( "string", query.getField() );
    }

    @Test
    public void shouldBuildRangeSeekByPrefixQueryForStrings() throws Exception
    {
        // given
        ConstantScoreQuery query = (ConstantScoreQuery) LuceneDocumentStructure.newRangeSeekByPrefixQuery( "Prefix" );
        MultiTermQuery prefixQuery = (MultiTermQuery) query.getQuery();

        // then
        assertThat("Should contain term value", prefixQuery.toString(), containsString( "Prefix" ) );
    }
}
