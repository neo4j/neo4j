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
package org.neo4j.kernel.api.impl.schema;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.api.impl.LuceneTestUtil.documentRepresentingProperties;
import static org.neo4j.kernel.api.impl.LuceneTestUtil.newSeekQuery;
import static org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure.NODE_ID_KEY;
import static org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure.useFieldForUniquenessVerification;
import static org.neo4j.kernel.api.impl.schema.ValueEncoding.String;

class LuceneDocumentStructureTest
{
    @Test
    void stringWithMaximumLengthShouldBeAllowed()
    {
        String longestString = RandomStringUtils.randomAscii( IndexWriter.MAX_TERM_LENGTH );
        Document document = documentRepresentingProperties( (long) 123, longestString );
        assertEquals( longestString, document.getField( String.key( 0 ) ).stringValue() );
    }

    @Test
    void shouldBuildDocumentRepresentingStringProperty()
    {
        // given
        Document document = documentRepresentingProperties( (long) 123, "hello" );

        // then
        assertEquals( "123", document.get( NODE_ID_KEY ) );
        assertEquals( "hello", document.get( String.key( 0 ) ) );
    }

    @Test
    void shouldBuildDocumentRepresentingMultipleStringProperties()
    {
        // given
        String[] values = new String[]{"hello", "world"};
        Document document = documentRepresentingProperties( 123, values );

        // then
        assertEquals( "123", document.get( NODE_ID_KEY ) );
        assertThat( document.get( String.key( 0 ) ), equalTo( values[0] ) );
        assertThat( document.get( String.key( 1 ) ), equalTo( values[1] ) );
    }

    @Test
    void shouldBuildQueryRepresentingStringProperty()
    {
        // given
        BooleanQuery booleanQuery = (BooleanQuery) newSeekQuery( "Characters" );
        ConstantScoreQuery query = (ConstantScoreQuery) booleanQuery.clauses().get( 0 ).getQuery();

        // then
        assertEquals( "Characters", ((TermQuery) query.getQuery()).getTerm().text() );
    }

    @Test
    void shouldBuildQueryRepresentingMultipleProperties()
    {
        // given
        BooleanQuery booleanQuery = (BooleanQuery) newSeekQuery( "foo", "bar" );

        ConstantScoreQuery fooScoreQuery = (ConstantScoreQuery) booleanQuery.clauses().get( 0 ).getQuery();
        TermQuery fooTermQuery = (TermQuery) fooScoreQuery.getQuery();

        ConstantScoreQuery barScoreQuery = (ConstantScoreQuery) booleanQuery.clauses().get( 1 ).getQuery();
        TermQuery barTermQuery = (TermQuery) barScoreQuery.getQuery();

        // then
        assertEquals( "foo", fooTermQuery.getTerm().text() );
        assertEquals( "bar", barTermQuery.getTerm().text() );
    }

    @Test
    void shouldBuildRangeSeekByStringQueryForStrings()
    {
        // given
        TermRangeQuery query = (TermRangeQuery) LuceneDocumentStructure
                .newRangeSeekByStringQuery( "foo", false, null, true );

        // then
        assertEquals( "string", query.getField() );
        assertEquals( "foo", query.getLowerTerm().utf8ToString() );
        assertFalse( query.includesLower() );
        assertNull( query.getUpperTerm() );
        assertTrue( query.includesUpper() );
    }

    @Test
    void shouldBuildWildcardQueries()
    {
        // given
        WildcardQuery query = (WildcardQuery) LuceneDocumentStructure.newWildCardStringQuery( "foo" );

        // then
        assertEquals( "string", query.getField() );
    }

    @Test
    void shouldBuildRangeSeekByPrefixQueryForStrings()
    {
        // given
        MultiTermQuery prefixQuery = (MultiTermQuery) LuceneDocumentStructure.newRangeSeekByPrefixQuery( "Prefix" );

        // then
        assertThat( "Should contain term value", prefixQuery.toString(), containsString( "Prefix" ) );
    }

    @Test
    void checkFieldUsageForUniquenessVerification()
    {
        assertFalse( useFieldForUniquenessVerification( "id" ) );
        assertFalse( useFieldForUniquenessVerification( "1number" ) );
        assertTrue( useFieldForUniquenessVerification( "number" ) );
        assertFalse( useFieldForUniquenessVerification( "1string" ) );
        assertFalse( useFieldForUniquenessVerification( "10string" ) );
        assertTrue( useFieldForUniquenessVerification( "string" ) );
    }
}
