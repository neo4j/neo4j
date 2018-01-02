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
package org.neo4j.kernel.api.impl.index;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.TestCase.assertEquals;
import static org.neo4j.kernel.api.impl.index.LuceneDocumentStructure.NODE_ID_KEY;
import static org.neo4j.kernel.api.impl.index.LuceneDocumentStructure.ValueEncoding.Array;
import static org.neo4j.kernel.api.impl.index.LuceneDocumentStructure.ValueEncoding.Bool;
import static org.neo4j.kernel.api.impl.index.LuceneDocumentStructure.ValueEncoding.Number;
import static org.neo4j.kernel.api.impl.index.LuceneDocumentStructure.ValueEncoding.String;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.NumericUtils;
import org.junit.Test;

public class LuceneDocumentStructureTest
{
    private final LuceneDocumentStructure documentStructure = new LuceneDocumentStructure();

    @Test
    public void shouldBuildDocumentRepresentingStringProperty() throws Exception
    {
        // given
        Fieldable fieldable = documentStructure.encodeAsFieldable( "hello" );
        Document document = documentStructure.newDocumentRepresentingProperty( 123, fieldable );

        // then
        assertEquals("123", document.get( NODE_ID_KEY ));
        assertEquals("hello", document.get( String.key() ));
    }

    @Test
    public void shouldBuildDocumentRepresentingBoolProperty() throws Exception
    {
        // given
        Fieldable fieldable = documentStructure.encodeAsFieldable( true );
        Document document = documentStructure.newDocumentRepresentingProperty( 123, fieldable );

        // then
        assertEquals("123", document.get( NODE_ID_KEY ));
        assertEquals("true", document.get( Bool.key() ));
    }

    @Test
    public void shouldBuildDocumentRepresentingNumberProperty() throws Exception
    {
        // given
        Fieldable fieldable = documentStructure.encodeAsFieldable( 12 );
        Document document = documentStructure.newDocumentRepresentingProperty( 123, fieldable );

        // then
        assertEquals("123", document.get( NODE_ID_KEY ));
        assertEquals( NumericUtils.doubleToPrefixCoded( 12.0 ), document.get( Number.key() ) );
    }

    @Test
    public void shouldBuildDocumentRepresentingArrayProperty() throws Exception
    {
        // given
        Fieldable fieldable = documentStructure.encodeAsFieldable( new Integer[]{1, 2, 3} );
        Document document = documentStructure.newDocumentRepresentingProperty( 123, fieldable );

        // then
        assertEquals("123", document.get( NODE_ID_KEY ));
        assertEquals("D1.0|2.0|3.0|", document.get( Array.key() ));
    }

    @Test
    public void shouldBuildQueryRepresentingBoolProperty() throws Exception
    {
        // given
        TermQuery query = documentStructure.newSeekQuery( true );

        // then
        assertEquals( "true", query.getTerm().text() );
    }

    @Test
    public void shouldBuildQueryRepresentingStringProperty() throws Exception
    {
        // given
        TermQuery query = documentStructure.newSeekQuery( "Characters" );

        // then
        assertEquals( "Characters", query.getTerm().text() );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldBuildQueryRepresentingNumberProperty() throws Exception
    {
        // given
        TermQuery query = documentStructure.newSeekQuery( 12 );

        // then
        assertEquals(  NumericUtils.doubleToPrefixCoded( 12.0 ), query.getTerm().text() );
    }

    @Test
    public void shouldBuildQueryRepresentingArrayProperty() throws Exception
    {
        // given
        TermQuery query = documentStructure.newSeekQuery( new Integer[]{1, 2, 3} );

        // then
        assertEquals( "D1.0|2.0|3.0|", query.getTerm().text() );
    }

    @Test
    public void shouldBuildRangeSeekByNumberQueryForStrings() throws Exception
    {
        // given
        TermRangeQuery query = documentStructure.newInclusiveNumericRangeSeekQuery( 12.0d, null );

        // then
        assertEquals( "number", query.getField() );
        assertEquals( NumericUtils.doubleToPrefixCoded( 12.0d ) , query.getLowerTerm() );
        assertEquals( true, query.includesLower() );
        assertEquals( null, query.getUpperTerm() );
        assertEquals( true, query.includesUpper() );
    }

    @Test
    public void shouldBuildRangeSeekByStringQueryForStrings() throws Exception
    {
        // given
        TermRangeQuery query = documentStructure.newRangeSeekByStringQuery( "foo", false, null, true );

        // then
        assertEquals( "string", query.getField() );
        assertEquals( "foo" , query.getLowerTerm() );
        assertEquals( false, query.includesLower() );
        assertEquals( null, query.getUpperTerm() );
        assertEquals( true, query.includesUpper() );
    }

    @Test
    public void shouldBuildRangeSeekByPrefixQueryForStrings() throws Exception
    {
        // given
        PrefixQuery query = documentStructure.newRangeSeekByPrefixQuery( "Prefix" );

        // then
        assertEquals( "Prefix", query.getPrefix().text() );
    }

    @Test
    public void shouldBuildScanQuery() throws Exception
    {
        // given
        MatchAllDocsQuery query = documentStructure.newScanQuery();

        // then
        assertNotNull( query );
    }
}
