/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.apache.lucene.document.Document;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.NumericUtils;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

import static org.neo4j.kernel.api.impl.index.LuceneDocumentStructure.NODE_ID_KEY;
import static org.neo4j.kernel.api.impl.index.LuceneDocumentStructure.ValueEncoding.Array;
import static org.neo4j.kernel.api.impl.index.LuceneDocumentStructure.ValueEncoding.Bool;
import static org.neo4j.kernel.api.impl.index.LuceneDocumentStructure.ValueEncoding.Number;
import static org.neo4j.kernel.api.impl.index.LuceneDocumentStructure.ValueEncoding.String;

public class LuceneDocumentStructureTest
{
    @Test
    public void shouldBuildDocumentRepresentingStringProperty() throws Exception
    {
        // given
        Document document = new LuceneDocumentStructure().newDocumentRepresentingProperty( 123, "hello" );

        // then
        assertEquals("123", document.get( NODE_ID_KEY ));
        assertEquals("hello", document.get( String.key() ));
    }

    @Test
    public void shouldBuildDocumentRepresentingBoolProperty() throws Exception
    {
        // given
        Document document = new LuceneDocumentStructure().newDocumentRepresentingProperty( 123, true );

        // then
        assertEquals("123", document.get( NODE_ID_KEY ));
        assertEquals("true", document.get( Bool.key() ));
    }

    @Test
    public void shouldBuildDocumentRepresentingNumberProperty() throws Exception
    {
        // given
        Document document = new LuceneDocumentStructure().newDocumentRepresentingProperty( 123, 12 );

        // then
        assertEquals("123", document.get( NODE_ID_KEY ));
        assertEquals( NumericUtils.doubleToPrefixCoded( 12.0 ), document.get( Number.key() ) );
    }

    @Test
    public void shouldBuildDocumentRepresentingArrayProperty() throws Exception
    {
        // given
        Document document = new LuceneDocumentStructure().newDocumentRepresentingProperty( 123, new Integer[] { 1,2,3 } );

        // then
        assertEquals("123", document.get( NODE_ID_KEY ));
        assertEquals("D1.0|2.0|3.0|", document.get( Array.key() ));
    }

    @Test
    public void shouldBuildQueryRepresentingBoolProperty() throws Exception
    {
        // given
        TermQuery query = (TermQuery) new LuceneDocumentStructure().newQuery( true );

        // then
        assertEquals( "true", query.getTerm().text() );
    }

    @Test
    public void shouldBuildQueryRepresentingStringProperty() throws Exception
    {
        // given
        TermQuery query = (TermQuery) new LuceneDocumentStructure().newQuery( "Characters" );

        // then
        assertEquals( "Characters", query.getTerm().text() );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldBuildQueryRepresentingNumberProperty() throws Exception
    {
        // given
        TermQuery query = (TermQuery) new LuceneDocumentStructure().newQuery( 12 );

        // then
        assertEquals(  NumericUtils.doubleToPrefixCoded( 12.0 ), query.getTerm().text() );
    }

    @Test
    public void shouldBuildQueryRepresentingArrayProperty() throws Exception
    {
        // given
        TermQuery query = (TermQuery) new LuceneDocumentStructure().newQuery( new Integer[] { 1,2,3 } );

        // then
        assertEquals( "D1.0|2.0|3.0|", query.getTerm().text() );
    }
}
