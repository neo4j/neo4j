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

import org.neo4j.csv.reader.BufferedCharSeeker;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.function.Factory;
import org.neo4j.unsafe.impl.batchimport.input.DuplicateHeaderException;
import org.neo4j.unsafe.impl.batchimport.input.MissingHeaderException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

import static org.neo4j.csv.reader.Readables.multipleSources;
import static org.neo4j.helpers.collection.IteratorUtil.array;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.TABS;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.data;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;

public class DataFactoriesTest
{
    @Test
    public void shouldParseDefaultNodeFileHeaderCorrectly() throws Exception
    {
        // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( new StringReader(
                "ID:ID,label-one:label,also-labels:LABEL,name,age:long" ) );
        IdType idType = IdType.STRING;
        Extractors extractors = new Extractors( ',' );

        // WHEN
        Header header = DataFactories.defaultFormatNodeFileHeader().create( seeker, COMMAS, idType );

        // THEN
        assertArrayEquals( array(
                entry( "ID", Type.ID, idType.extractor( extractors ) ),
                entry( "label-one", Type.LABEL, extractors.stringArray() ),
                entry( "also-labels", Type.LABEL, extractors.stringArray() ),
                entry( "name", Type.PROPERTY, extractors.string() ),
                entry( "age", Type.PROPERTY, extractors.long_() ) ), header.entries() );
        seeker.close();
    }

    @Test
    public void shouldParseDefaultRelationshipFileHeaderCorrectly() throws Exception
    {
        // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( new StringReader(
                "node one\tnode two\ttype\tdate:long\tmore:long[]" ) );
        IdType idType = IdType.ACTUAL;
        Extractors extractors = new Extractors( '\t' );

        // WHEN
        Header header = DataFactories.defaultFormatRelationshipFileHeader().create( seeker, TABS, idType );

        // THEN
        assertArrayEquals( array(
                entry( "node one", Type.START_NODE, idType.extractor( extractors ) ),
                entry( "node two", Type.END_NODE, idType.extractor( extractors ) ),
                entry( "type", Type.RELATIONSHIP_TYPE, extractors.string() ),
                entry( "date", Type.PROPERTY, extractors.long_() ),
                entry( "more", Type.PROPERTY, extractors.longArray() ) ), header.entries() );
        seeker.close();
    }

    @Test
    public void shouldHaveEmptyHeadersBeInterpretedAsIgnored() throws Exception
    {
        // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( new StringReader(
                "one:id\ttwo\t\tdate:long" ) );
        IdType idType = IdType.ACTUAL;
        Extractors extractors = new Extractors( '\t' );

        // WHEN
        Header header = DataFactories.defaultFormatNodeFileHeader().create( seeker, TABS, idType );

        // THEN
        assertArrayEquals( array(
                entry( "one", Type.ID, extractors.long_() ),
                entry( "two", Type.PROPERTY, extractors.string() ),
                entry( "", Type.IGNORE, null ),
                entry( "date", Type.PROPERTY, extractors.long_() ) ), header.entries() );
        seeker.close();
    }

    @Test
    public void shouldFailForDuplicatePropertyHeaderEntries() throws Exception
    {
        // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( new StringReader(
                "one:id\tname\tname:long" ) );
        IdType idType = IdType.ACTUAL;
        Extractors extractors = new Extractors( '\t' );

        // WHEN
        try
        {
            DataFactories.defaultFormatNodeFileHeader().create( seeker, TABS, idType );
            fail( "Should fail" );
        }
        catch ( DuplicateHeaderException e )
        {
            assertEquals( entry( "name", Type.PROPERTY, extractors.string() ), e.getFirst() );
            assertEquals( entry( "name", Type.PROPERTY, extractors.long_() ), e.getOther() );
        }
        seeker.close();
    }

    @Test
    public void shouldFailForDuplicateIdHeaderEntries() throws Exception
    {
        // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( new StringReader(
                "one:id\ttwo:id" ) );
        IdType idType = IdType.ACTUAL;
        Extractors extractors = new Extractors( '\t' );

        // WHEN
        try
        {
            DataFactories.defaultFormatNodeFileHeader().create( seeker, TABS, idType );
            fail( "Should fail" );
        }
        catch ( DuplicateHeaderException e )
        {
            assertEquals( entry( "one", Type.ID, extractors.long_() ), e.getFirst() );
            assertEquals( entry( "two", Type.ID, extractors.long_() ), e.getOther() );
        }
        seeker.close();
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
            DataFactories.defaultFormatNodeFileHeader().create( seeker, TABS, IdType.ACTUAL );
            fail( "Should fail" );
        }
        catch ( MissingHeaderException e )
        {
            assertEquals( Type.ID, e.getMissingType() );
        }
        seeker.close();
    }

    @Test
    public void shouldParseHeaderFromSeparateReader() throws Exception
    {
        // GIVEN
        CharSeeker dataSeeker = mock( CharSeeker.class );
        Header.Factory headerFactory =
                defaultFormatNodeFileHeader( new StringReader( "id:ID\tname:String\tbirth_date:long" ) );
        Extractors extractors = new Extractors( ';' );

        // WHEN
        Header header = headerFactory.create( dataSeeker, TABS, IdType.ACTUAL );

        // THEN
        assertArrayEquals( array(
                entry( "id", Type.ID, extractors.long_() ),
                entry( "name", Type.PROPERTY, extractors.string() ),
                entry( "birth_date", Type.PROPERTY, extractors.long_() ) ), header.entries() );
        verifyZeroInteractions( dataSeeker );
        dataSeeker.close();
    }

    @Test
    public void shouldParseHeaderFromFirstLineOfFirstInputFile() throws Exception
    {
        // GIVEN
        final Readable firstSource = new StringReader( "id:ID\tname:String\tbirth_date:long" );
        final Readable secondSource = new StringReader( "0\tThe node\t123456789" );
        DataFactory dataFactory = data( new Factory<Readable>()
        {
            @Override
            public Readable newInstance()
            {
                return multipleSources( firstSource, secondSource );
            }
        } );
        Header.Factory headerFactory = defaultFormatNodeFileHeader();
        Extractors extractors = new Extractors( ';' );

        // WHEN
        CharSeeker seeker = dataFactory.create( TABS );
        Header header = headerFactory.create( seeker, TABS, IdType.ACTUAL );

        // THEN
        assertArrayEquals( array(
                entry( "id", Type.ID, extractors.long_() ),
                entry( "name", Type.PROPERTY, extractors.string() ),
                entry( "birth_date", Type.PROPERTY, extractors.long_() ) ), header.entries() );
        seeker.close();
    }

    private Header.Entry entry( String name, Type type, Extractor<?> extractor )
    {
        return new Header.Entry( name, type, extractor );
    }
}
