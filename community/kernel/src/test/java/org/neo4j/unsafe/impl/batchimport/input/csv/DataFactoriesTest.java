/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import java.io.StringReader;

import org.junit.Test;

import org.neo4j.unsafe.impl.batchimport.input.DuplicateHeaderException;
import org.neo4j.unsafe.impl.batchimport.input.MissingHeaderException;
import org.neo4j.unsafe.impl.batchimport.input.csv.reader.BufferedCharSeeker;
import org.neo4j.unsafe.impl.batchimport.input.csv.reader.CharSeeker;
import org.neo4j.unsafe.impl.batchimport.input.csv.reader.Extractor;
import org.neo4j.unsafe.impl.batchimport.input.csv.reader.Extractors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.collection.IteratorUtil.array;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.TABS;

public class DataFactoriesTest
{
    @Test
    public void shouldParseDefaultNodeFileHeaderCorrectly() throws Exception
    {
        // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( new StringReader(
                "ID:ID,label-one:label,also-labels:LABEL,name,age:long" ) );
        IdType idType = IdType.STRING;

        // WHEN
        Header header = DataFactories.defaultFormatNodeFileHeader().create( seeker, COMMAS, idType.extractor() );

        // THEN
        Extractors extractors = new Extractors( ',' );
        assertArrayEquals( array(
                entry( "ID", Type.ID, idType.extractor() ),
                entry( "label-one", Type.LABEL, extractors.stringArray() ),
                entry( "also-labels", Type.LABEL, extractors.stringArray() ),
                entry( "name", Type.PROPERTY, Extractors.STRING ),
                entry( "age", Type.PROPERTY, Extractors.LONG ) ), header.entries() );
    }

    @Test
    public void shouldParseDefaultRelationshipFileHeaderCorrectly() throws Exception
    {
        // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( new StringReader(
                "node one\tnode two\ttype\tdate:long\tmore:long[]" ) );
        IdType idType = IdType.ACTUAL;

        // WHEN
        Header header = DataFactories.defaultFormatRelationshipFileHeader().create( seeker, TABS, idType.extractor() );

        // THEN
        Extractors extractors = new Extractors( '\t' );
        assertArrayEquals( array(
                entry( "node one", Type.START_NODE, idType.extractor() ),
                entry( "node two", Type.END_NODE, idType.extractor() ),
                entry( "type", Type.RELATIONSHIP_TYPE, Extractors.STRING ),
                entry( "date", Type.PROPERTY, Extractors.LONG ),
                entry( "more", Type.PROPERTY, extractors.longArray() ) ), header.entries() );
    }

    @Test
    public void shouldHaveEmptyHeadersBeInterpretedAsIgnored() throws Exception
    {
        // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( new StringReader(
                "one:id\ttwo\t\tdate:long" ) );
        IdType idType = IdType.ACTUAL;

        // WHEN
        Header header = DataFactories.defaultFormatNodeFileHeader().create( seeker, TABS, idType.extractor() );

        // THEN
        assertArrayEquals( array(
                entry( "one", Type.ID, Extractors.LONG ),
                entry( "two", Type.PROPERTY, Extractors.STRING ),
                entry( "", Type.IGNORE, null ),
                entry( "date", Type.PROPERTY, Extractors.LONG ) ), header.entries() );
    }

    @Test
    public void shouldFailForDuplicatePropertyHeaderEntries() throws Exception
    {
        // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( new StringReader(
                "one:id\tname\tname:long" ) );
        IdType idType = IdType.ACTUAL;

        // WHEN
        try
        {
            DataFactories.defaultFormatNodeFileHeader().create( seeker, TABS, idType.extractor() );
            fail( "Should fail" );
        }
        catch ( DuplicateHeaderException e )
        {
            assertEquals( entry( "name", Type.PROPERTY, Extractors.STRING ), e.getFirst() );
            assertEquals( entry( "name", Type.PROPERTY, Extractors.LONG ), e.getOther() );
        }
    }

    @Test
    public void shouldFailForDuplicateIdHeaderEntries() throws Exception
    {
        // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( new StringReader(
                "one:id\ttwo:id" ) );
        IdType idType = IdType.ACTUAL;

        // WHEN
        try
        {
            DataFactories.defaultFormatNodeFileHeader().create( seeker, TABS, idType.extractor() );
            fail( "Should fail" );
        }
        catch ( DuplicateHeaderException e )
        {
            assertEquals( entry( "one", Type.ID, Extractors.LONG ), e.getFirst() );
            assertEquals( entry( "two", Type.ID, Extractors.LONG ), e.getOther() );
        }
    }

    @Test
    public void shouldFailForMissingIdHeaderEntry() throws Exception
    {
        // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( new StringReader(
                "one\ttwo" ) );

        // WHEN
        try
        {
            DataFactories.defaultFormatNodeFileHeader().create( seeker, TABS, Extractors.LONG );
            fail( "Should fail" );
        }
        catch ( MissingHeaderException e )
        {
            assertEquals( Type.ID, e.getMissingType() );
        }
    }

    private Header.Entry entry( String name, Type type, Extractor<?> extractor )
    {
        return new Header.Entry( name, type, extractor );
    }
}
