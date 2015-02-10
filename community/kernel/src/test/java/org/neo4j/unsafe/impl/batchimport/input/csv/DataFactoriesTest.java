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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import org.junit.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.neo4j.csv.reader.BufferedCharSeeker;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.function.Factory;
import org.neo4j.function.Functions;
import org.neo4j.unsafe.impl.batchimport.input.DuplicateHeaderException;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

import static org.neo4j.csv.reader.Readables.multipleSources;
import static org.neo4j.csv.reader.Readables.wrap;
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
        CharSeeker seeker = new BufferedCharSeeker( wrap( new StringReader(
                "ID:ID,label-one:label,also-labels:LABEL,name,age:long" ) ) );
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
        CharSeeker seeker = new BufferedCharSeeker( wrap( new StringReader(
                ":START_ID\t:END_ID\ttype:TYPE\tdate:long\tmore:long[]" ) ) );
        IdType idType = IdType.ACTUAL;
        Extractors extractors = new Extractors( '\t' );

        // WHEN
        Header header = DataFactories.defaultFormatRelationshipFileHeader().create( seeker, TABS, idType );

        // THEN
        assertArrayEquals( array(
                entry( null, Type.START_ID, idType.extractor( extractors ) ),
                entry( null, Type.END_ID, idType.extractor( extractors ) ),
                entry( "type", Type.TYPE, extractors.string() ),
                entry( "date", Type.PROPERTY, extractors.long_() ),
                entry( "more", Type.PROPERTY, extractors.longArray() ) ), header.entries() );
        seeker.close();
    }

    @Test
    public void shouldHaveEmptyHeadersBeInterpretedAsIgnored() throws Exception
    {
        // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( wrap( new StringReader( "one:id\ttwo\t\tdate:long" ) ) );
        IdType idType = IdType.ACTUAL;
        Extractors extractors = new Extractors( '\t' );

        // WHEN
        Header header = DataFactories.defaultFormatNodeFileHeader().create( seeker, TABS, idType );

        // THEN
        assertArrayEquals( array(
                entry( "one", Type.ID, extractors.long_() ),
                entry( "two", Type.PROPERTY, extractors.string() ),
                entry( null, Type.IGNORE, null ),
                entry( "date", Type.PROPERTY, extractors.long_() ) ), header.entries() );
        seeker.close();
    }

    @Test
    public void shouldFailForDuplicatePropertyHeaderEntries() throws Exception
    {
        // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( wrap( new StringReader( "one:id\tname\tname:long" ) ) );
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
        CharSeeker seeker = new BufferedCharSeeker( wrap( new StringReader( "one:id\ttwo:id" ) ) );
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
    public void shouldAllowMissingIdHeaderEntry() throws Exception
    {
        // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( wrap( new StringReader( "one\ttwo" ) ) );
        Extractors extractors = new Extractors( ';' );

        // WHEN
        Header header = DataFactories.defaultFormatNodeFileHeader().create( seeker, TABS, IdType.ACTUAL );

        // THEN
        assertArrayEquals( array(
                entry( "one", Type.PROPERTY, extractors.string() ),
                entry( "two", Type.PROPERTY, extractors.string() ) ), header.entries() );
        seeker.close();
    }

    @Test
    public void shouldParseHeaderFromSeparateReader() throws Exception
    {
        // GIVEN
        CharSeeker dataSeeker = mock( CharSeeker.class );
        Header.Factory headerFactory =
                defaultFormatNodeFileHeader( wrap( new StringReader( "id:ID\tname:String\tbirth_date:long" ) ) );
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
        final Reader firstSource = new StringReader( "id:ID\tname:String\tbirth_date:long" );
        final Reader secondSource = new StringReader( "0\tThe node\t123456789" );
        DataFactory<InputNode> dataFactory = data( Functions.<InputNode>identity(), new Factory<CharReadable>()
        {
            @Override
            public CharReadable newInstance()
            {
                try
                {
                    return multipleSources( firstSource, secondSource );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        } );
        Header.Factory headerFactory = defaultFormatNodeFileHeader();
        Extractors extractors = new Extractors( ';' );

        // WHEN
        CharSeeker seeker = dataFactory.create( TABS ).stream();
        Header header = headerFactory.create( seeker, TABS, IdType.ACTUAL );

        // THEN
        assertArrayEquals( array(
                entry( "id", Type.ID, extractors.long_() ),
                entry( "name", Type.PROPERTY, extractors.string() ),
                entry( "birth_date", Type.PROPERTY, extractors.long_() ) ), header.entries() );
        seeker.close();
    }

    @Test
    public void shouldParseGroupName() throws Exception
    {
     // GIVEN
        CharSeeker seeker = new BufferedCharSeeker( wrap( new StringReader(
                ":START_ID(GroupOne)\t:END_ID(GroupTwo)\ttype:TYPE\tdate:long\tmore:long[]" ) ) );
        IdType idType = IdType.ACTUAL;
        Extractors extractors = new Extractors( '\t' );

        // WHEN
        Header header = DataFactories.defaultFormatRelationshipFileHeader().create( seeker, TABS, idType );

        // THEN
        assertArrayEquals( array(
                entry( null, Type.START_ID, "GroupOne", idType.extractor( extractors ) ),
                entry( null, Type.END_ID, "GroupTwo", idType.extractor( extractors ) ),
                entry( "type", Type.TYPE, extractors.string() ),
                entry( "date", Type.PROPERTY, extractors.long_() ),
                entry( "more", Type.PROPERTY, extractors.longArray() ) ), header.entries() );
        seeker.close();
    }

    private Header.Entry entry( String name, Type type, Extractor<?> extractor )
    {
        return entry( name, type, null, extractor );
    }

    private Header.Entry entry( String name, Type type, String groupName, Extractor<?> extractor )
    {
        return new Header.Entry( name, type, groupName, extractor );
    }
}
