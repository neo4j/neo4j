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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.collection.RawIterator;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Readables;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;
import org.neo4j.unsafe.impl.batchimport.input.InputException;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.csv.reader.Readables.wrap;
import static org.neo4j.helpers.ArrayUtil.union;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.unsafe.impl.batchimport.input.Collectors.silentBadCollector;
import static org.neo4j.unsafe.impl.batchimport.input.Group.GLOBAL;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntity.NO_PROPERTIES;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.NO_DECORATOR;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.additiveLabels;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.defaultRelationshipType;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.datas;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatRelationshipFileHeader;

@RunWith( Parameterized.class )
public class CsvInputTest
{
    @Parameters
    public static Collection<Boolean> data()
    {
        // multi-line fields
        return asList(
                Boolean.TRUE,
                Boolean.FALSE );
    }

    @Rule
    public final RandomRule random = new RandomRule();
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory( getClass() );
    private final Extractors extractors = new Extractors( ',' );
    @Parameter
    public Boolean allowMultilineFields;

    private final InputEntity visitor = new InputEntity();
    private final Groups groups = new Groups();
    private InputChunk chunk;
    private InputIterator referenceData;

    @Test
    public void shouldProvideNodesFromCsvInput() throws Exception
    {
        // GIVEN
        IdType idType = IdType.ACTUAL;
        Iterable<DataFactory> data = dataIterable( data( "123,Mattias Persson,HACKER" ) );
        Input input = new CsvInput(
                data,
                header( entry( null, Type.ID, idType.extractor( extractors ) ),
                        entry( "name", Type.PROPERTY, extractors.string() ),
                        entry( "labels", Type.LABEL, extractors.string() ) ),
                        datas(), defaultFormatRelationshipFileHeader(), idType, config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN/THEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            assertNextNode( nodes, 123L, properties( "name", "Mattias Persson" ), labels( "HACKER" ) );
            assertFalse( chunk.next( visitor ) );
        }
    }

    @Test
    public void shouldProvideRelationshipsFromCsvInput() throws Exception
    {
        // GIVEN
        IdType idType = IdType.STRING;
        Iterable<DataFactory> data = dataIterable( data(
              "node1,node2,KNOWS,1234567\n" +
              "node2,node10,HACKS,987654" ) );
        Input input = new CsvInput( datas(), defaultFormatNodeFileHeader(),
                data,
                header( entry( "from", Type.START_ID, idType.extractor( extractors ) ),
                        entry( "to", Type.END_ID, idType.extractor( extractors ) ),
                        entry( "type", Type.TYPE, extractors.string() ),
                        entry( "since", Type.PROPERTY, extractors.long_() ) ), idType, config( COMMAS ),
                        silentBadCollector( 0 ) );

        // WHEN/THEN
        try ( InputIterator relationships = input.relationships().iterator() )
        {
            assertNextRelationship( relationships, "node1", "node2", "KNOWS", properties( "since", 1234567L ) );
            assertNextRelationship( relationships, "node2", "node10", "HACKS", properties( "since", 987654L ) );
        }
    }

    @Test
    public void shouldCloseDataIteratorsInTheEnd() throws Exception
    {
        // GIVEN
        CapturingDataFactories nodeData = new CapturingDataFactories( config -> charReader( "1" ), NO_DECORATOR );
        CapturingDataFactories relationshipData = new CapturingDataFactories( config -> charReader( "1,1" ),
                defaultRelationshipType( "TYPE" ) );

        IdType idType = IdType.STRING;
        Input input = new CsvInput(
                nodeData, header(
                        entry( null, Type.ID, idType.extractor( extractors ) ) ),
                relationshipData, header(
                        entry( null, Type.START_ID, idType.extractor( extractors ) ),
                        entry( null, Type.END_ID, idType.extractor( extractors ) ) ),
                idType, config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator iterator = input.nodes().iterator() )
        {
            readNext( iterator );
        }
        try ( InputIterator iterator = input.relationships().iterator() )
        {
            readNext( iterator );
        }

        // THEN
        assertClosed( nodeData.last() );
        assertClosed( relationshipData.last() );
    }

    private void assertClosed( CharReadable reader )
    {
        try
        {
            reader.read( new char[1], 0, 1 );
            fail( reader + " not closed" );
        }
        catch ( IOException e )
        {
            assertTrue( e.getMessage().contains( "closed" ) );
        }
    }

    @Test
    public void shouldCopeWithLinesThatHasTooFewValuesButStillValidates() throws Exception
    {
        // GIVEN
        Iterable<DataFactory> data = dataIterable( data( "1,ultralisk,ZERG,10\n" +
                                                         "2,corruptor,ZERG\n" +
                                                         "3,mutalisk,ZERG,3" ) );
        Input input = new CsvInput(
                data,
                header(
                      entry( null, Type.ID, extractors.long_() ),
                      entry( "unit", Type.PROPERTY, extractors.string() ),
                      entry( "type", Type.LABEL, extractors.string() ),
                      entry( "kills", Type.PROPERTY, extractors.int_() ) ),
                datas(), defaultFormatRelationshipFileHeader(), IdType.ACTUAL, config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            assertNextNode( nodes, 1L, new Object[] { "unit", "ultralisk", "kills", 10 }, labels( "ZERG" ) );
            assertNextNode( nodes, 2L, new Object[] { "unit", "corruptor" }, labels( "ZERG" ) );
            assertNextNode( nodes, 3L, new Object[] { "unit", "mutalisk", "kills", 3 }, labels( "ZERG" ) );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldIgnoreValuesAfterHeaderEntries() throws Exception
    {
        // GIVEN
        Iterable<DataFactory> data = dataIterable( data( "1,zergling,bubble,bobble\n" +
                                                         "2,scv,pun,intended" ) );
        Input input = new CsvInput(
                data,
                header(
                      entry( null, Type.ID, extractors.long_() ),
                      entry( "name", Type.PROPERTY, extractors.string() ) ),
                datas(), defaultFormatRelationshipFileHeader(), IdType.ACTUAL, config( COMMAS ), silentBadCollector( 4 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            assertNextNode( nodes, 1L, new Object[] { "name", "zergling" }, labels() );
            assertNextNode( nodes, 2L, new Object[] { "name", "scv" }, labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldHandleMultipleInputGroups() throws Exception
    {
        // GIVEN multiple input groups, each with their own, specific, header
        DataFactory group1 = data( ":ID,name,kills:int,health:int\n" +
                                   "1,Jim,10,100\n" +
                                   "2,Abathur,0,200\n" );
        DataFactory group2 = data( ":ID,type\n" +
                                   "3,zergling\n" +
                                   "4,csv\n" );
        Iterable<DataFactory> data = dataIterable( group1, group2 );
        Input input = new CsvInput( data, defaultFormatNodeFileHeader(),
                                    datas(), defaultFormatRelationshipFileHeader(),
                                    IdType.STRING, config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN iterating over them, THEN the expected data should come out
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            assertNextNode( nodes, "1", properties( "name", "Jim", "kills", 10, "health", 100 ), labels() );
            assertNextNode( nodes, "2", properties( "name", "Abathur", "kills", 0, "health", 200 ), labels() );
            assertNextNode( nodes, "3", properties( "type", "zergling" ), labels() );
            assertNextNode( nodes, "4", properties( "type", "csv" ), labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldProvideAdditiveLabels() throws Exception
    {
        // GIVEN
        String[] addedLabels = {"Two", "AddTwo"};
        DataFactory data = data( ":ID,name,:LABEL\n" +
                                 "0,First,\n" +
                                 "1,Second,One\n" +
                                 "2,Third,One;Two",
                                 additiveLabels( addedLabels ) );
        Iterable<DataFactory> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(),
                datas(), defaultFormatRelationshipFileHeader(), IdType.ACTUAL, config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN/THEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            assertNextNode( nodes, 0L, properties( "name", "First" ),
                    labels( addedLabels ) );
            assertNextNode( nodes, 1L, properties( "name", "Second" ),
                    labels( union( new String[] {"One"}, addedLabels ) ) );
            assertNextNode( nodes, 2L, properties( "name", "Third" ),
                    labels( union( new String[] {"One"}, addedLabels ) ) );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldProvideDefaultRelationshipType() throws Exception
    {
        // GIVEN
        String defaultType = "DEFAULT";
        String customType = "CUSTOM";
        DataFactory data = data( ":START_ID,:END_ID,:TYPE\n" +
                "0,1,\n" +
                "1,2," + customType + "\n" +
                "2,1," + defaultType,
                defaultRelationshipType( defaultType ) );
        Iterable<DataFactory> dataIterable = dataIterable( data );
        Input input = new CsvInput( datas(), defaultFormatNodeFileHeader(),
                dataIterable, defaultFormatRelationshipFileHeader(), IdType.ACTUAL, config( COMMAS ),
                silentBadCollector( 0 ) );

        // WHEN/THEN
        try ( InputIterator relationships = input.relationships().iterator() )
        {
            assertNextRelationship( relationships, 0L, 1L, defaultType, NO_PROPERTIES );
            assertNextRelationship( relationships, 1L, 2L, customType, NO_PROPERTIES );
            assertNextRelationship( relationships, 2L, 1L, defaultType, NO_PROPERTIES );
            assertFalse( readNext( relationships ) );
        }
    }

    @Test
    public void shouldAllowNodesWithoutIdHeader() throws Exception
    {
        // GIVEN
        DataFactory data = data(
                "name:string,level:int\n" +
                "Mattias,1\n" +
                "Johan,2\n" );
        Iterable<DataFactory> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), datas(), defaultFormatRelationshipFileHeader(),
                IdType.STRING, config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            assertNextNode( nodes, null, new Object[] {"name", "Mattias", "level", 1}, labels() );
            assertNextNode( nodes, null, new Object[] {"name", "Johan", "level", 2}, labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldAllowSomeNodesToBeAnonymous() throws Exception
    {
        // GIVEN
        DataFactory data = data(
                ":ID,name:string,level:int\n" +
                "abc,Mattias,1\n" +
                ",Johan,2\n" ); // this node is anonymous
        Iterable<DataFactory> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), datas(), defaultFormatRelationshipFileHeader(),
                IdType.STRING, config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            assertNextNode( nodes, "abc", new Object[] {"name", "Mattias", "level", 1}, labels() );
            assertNextNode( nodes, null, new Object[] {"name", "Johan", "level", 2}, labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldAllowNodesToBeAnonymousEvenIfIdHeaderIsNamed() throws Exception
    {
        // GIVEN
        DataFactory data = data(
                "id:ID,name:string,level:int\n" +
                "abc,Mattias,1\n" +
                ",Johan,2\n" ); // this node is anonymous
        Iterable<DataFactory> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), datas(), defaultFormatRelationshipFileHeader(),
                IdType.STRING, config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            assertNextNode( nodes, "abc", new Object[] {"id", "abc", "name", "Mattias", "level", 1}, labels() );
            assertNextNode( nodes, null, new Object[] {"name", "Johan", "level", 2}, labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldNotHaveIdSetAsPropertyIfIdHeaderEntryIsNamedForActualIds() throws Exception
    {
        // GIVEN
        DataFactory data = data(
                "myId:ID,name:string,level:int\n" +
                "0,Mattias,1\n" +
                "1,Johan,2\n" ); // this node is anonymous
        Iterable<DataFactory> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(),
                datas(), defaultFormatRelationshipFileHeader(), IdType.ACTUAL,
                config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            assertNextNode( nodes, 0L, new Object[] {"name", "Mattias", "level", 1}, labels() );
            assertNextNode( nodes, 1L, new Object[] {"name", "Johan", "level", 2}, labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldIgnoreEmptyPropertyValues() throws Exception
    {
        // GIVEN
        DataFactory data = data(
                ":ID,name,extra\n" +
                "0,Mattias,\n" +            // here we leave out "extra" property
                "1,Johan,Additional\n" );
        Iterable<DataFactory> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), datas(), defaultFormatRelationshipFileHeader(),
                IdType.ACTUAL, config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            assertNextNode( nodes, 0L, new Object[] {"name", "Mattias"}, labels() );
            assertNextNode( nodes, 1L, new Object[] {"name", "Johan", "extra", "Additional"}, labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldIgnoreEmptyIntPropertyValues() throws Exception
    {
        // GIVEN
        DataFactory data = data(
                ":ID,name,extra:int\n" +
                "0,Mattias,\n" +            // here we leave out "extra" property
                "1,Johan,10\n" );
        Iterable<DataFactory> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), datas(), defaultFormatRelationshipFileHeader(),
                IdType.ACTUAL, config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            assertNextNode( nodes, 0L, new Object[] {"name", "Mattias"}, labels() );
            assertNextNode( nodes, 1L, new Object[] {"name", "Johan", "extra", 10}, labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldParsePointPropertyValues() throws Exception
    {
        // GIVEN
        DataFactory data = data(
                ":ID,name,point:Point\n" +
                "0,Mattias,\"{x: 2.7, y:3.2 }\"\n" +
                "1,Johan,\" { height :0.01 ,longitude:5, latitude : -4.2 } \"\n" );
        Iterable<DataFactory> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), datas(), defaultFormatRelationshipFileHeader(),
                IdType.ACTUAL, config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            assertNextNode( nodes, 0L, new Object[]{"name", "Mattias", "point",
                    Values.pointValue( CoordinateReferenceSystem.Cartesian, 2.7, 3.2) }, labels() );
            assertNextNode( nodes, 1L, new Object[]{"name", "Johan", "point",
                    Values.pointValue( CoordinateReferenceSystem.WGS84_3D, 5, -4.2, 0.01)}, labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldNotParsePointPropertyValuesWithDuplicateKeys() throws Exception
    {
        // GIVEN
        DataFactory data = data( ":ID,name,point:Point\n" + "1,Johan,\" { height :0.01 ,longitude:5, latitude : -4.2, latitude : 4.2 } \"\n" );
        Iterable<DataFactory> dataIterable = dataIterable( data );
        Input input =
                new CsvInput( dataIterable, defaultFormatNodeFileHeader(), datas(), defaultFormatRelationshipFileHeader(), IdType.ACTUAL, config( COMMAS ),
                        silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            readNext( nodes );
            fail( "Should have failed when key assigned multiple times, but didn't." );
        }
        catch ( InputException ignore )
        {
            // this is fine
        }
    }

    @Test
    public void shouldParsePointPropertyValuesWithCRSInHeader() throws Exception
    {
        // GIVEN
        DataFactory data = data(
                ":ID,name,point:Point{crs:WGS-84-3D}\n" +
                        "0,Johan,\" { height :0.01 ,longitude:5, latitude : -4.2 } \"\n" );
        Iterable<DataFactory> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), datas(), defaultFormatRelationshipFileHeader(),
                IdType.ACTUAL, config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            assertNextNode( nodes, 0L, new Object[]{"name", "Johan", "point",
                    Values.pointValue( CoordinateReferenceSystem.WGS84_3D, 5, -4.2, 0.01)}, labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldUseHeaderInformationToParsePoint() throws Exception
    {
        // GIVEN
        DataFactory data = data(
                ":ID,name,point:Point{crs:WGS-84}\n" +
                        "0,Johan,\" { x :1 ,y:2 } \"\n" );
        Iterable<DataFactory> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), datas(), defaultFormatRelationshipFileHeader(),
                IdType.ACTUAL, config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            assertNextNode( nodes, 0L, new Object[]{"name", "Johan", "point",
                    Values.pointValue( CoordinateReferenceSystem.WGS84, 1, 2)}, labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldParseDatePropertyValues() throws Exception
    {
        // GIVEN
        DataFactory data = data(
                ":ID,name,date:Date\n" +
                "0,Mattias,2018-02-27\n" +
                "1,Johan,2018-03-01\n" );
        Iterable<DataFactory> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), datas(),
                defaultFormatRelationshipFileHeader(),
                IdType.ACTUAL, config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            assertNextNode( nodes, 0L, new Object[]{"name", "Mattias", "date",
                    DateValue.date( 2018, 2, 27 )}, labels() );
            assertNextNode( nodes, 1L, new Object[]{"name", "Johan", "date",
                    DateValue.date( 2018, 3, 1 )}, labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldParseTimePropertyValues() throws Exception
    {
        // GIVEN
        DataFactory data = data(
                ":ID,name,time:Time\n" +
                "0,Mattias,13:37\n" +
                "1,Johan,\"16:20:01\"\n" +
                "2,Bob,07:30-05:00\n" );
        Iterable<DataFactory> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), datas(),
                defaultFormatRelationshipFileHeader(),
                IdType.ACTUAL, config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            assertNextNode( nodes, 0L, new Object[]{"name", "Mattias", "time",
                    TimeValue.time( 13, 37, 0, 0, "+00:00" )}, labels() );
            assertNextNode( nodes, 1L, new Object[]{"name", "Johan", "time",
                    TimeValue.time( 16, 20, 1, 0, "+00:00" )}, labels() );
            assertNextNode( nodes, 2L, new Object[]{"name", "Bob", "time",
                    TimeValue.time( 7, 30, 0, 0, "-05:00" )}, labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldParseTimePropertyValuesWithTimezoneInHeader() throws Exception
    {
        // GIVEN
        DataFactory data = data(
                ":ID,name,time:Time{timezone:+02:00}\n" +
                        "0,Mattias,13:37\n" +
                        "1,Johan,\"16:20:01\"\n" +
                        "2,Bob,07:30-05:00\n" );
        Iterable<DataFactory> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), datas(),
                defaultFormatRelationshipFileHeader(),
                IdType.ACTUAL, config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            assertNextNode( nodes, 0L, new Object[]{"name", "Mattias", "time",
                    TimeValue.time( 13, 37, 0, 0, "+02:00" )}, labels() );
            assertNextNode( nodes, 1L, new Object[]{"name", "Johan", "time",
                    TimeValue.time( 16, 20, 1, 0, "+02:00" )}, labels() );
            assertNextNode( nodes, 2L, new Object[]{"name", "Bob", "time",
                    TimeValue.time( 7, 30, 0, 0, "-05:00" )}, labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldParseDateTimePropertyValues() throws Exception
    {
        // GIVEN
        DataFactory data = data(
                ":ID,name,time:DateTime\n" +
                "0,Mattias,2018-02-27T13:37\n" +
                "1,Johan,\"2018-03-01T16:20:01\"\n" +
                "2,Bob,1981-05-11T07:30-05:00\n" );

        Iterable<DataFactory> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), datas(),
                defaultFormatRelationshipFileHeader(),
                IdType.ACTUAL, config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            assertNextNode( nodes, 0L, new Object[]{"name", "Mattias", "time",
                    DateTimeValue.datetime( 2018, 2, 27, 13, 37, 0, 0, "+00:00" )}, labels() );
            assertNextNode( nodes, 1L, new Object[]{"name", "Johan", "time",
                    DateTimeValue.datetime( 2018, 3, 1, 16, 20, 1, 0, "+00:00" )}, labels() );
            assertNextNode( nodes, 2L, new Object[]{"name", "Bob", "time",
                    DateTimeValue.datetime( 1981, 5, 11, 7, 30, 0, 0, "-05:00" )}, labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldParseDateTimePropertyValuesWithTimezoneInHeader() throws Exception
    {
        // GIVEN
        DataFactory data = data(
                ":ID,name,time:DateTime{timezone:Europe/Stockholm}\n" +
                "0,Mattias,2018-02-27T13:37\n" +
                "1,Johan,\"2018-03-01T16:20:01\"\n" +
                "2,Bob,1981-05-11T07:30-05:00\n" );

        Iterable<DataFactory> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), datas(),
                defaultFormatRelationshipFileHeader(),
                IdType.ACTUAL, config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            assertNextNode( nodes, 0L, new Object[]{"name", "Mattias", "time",
                    DateTimeValue.datetime( 2018, 2, 27, 13, 37, 0, 0, "Europe/Stockholm" )}, labels() );
            assertNextNode( nodes, 1L, new Object[]{"name", "Johan", "time",
                    DateTimeValue.datetime( 2018, 3, 1, 16, 20, 1, 0, "Europe/Stockholm" )}, labels() );
            assertNextNode( nodes, 2L, new Object[]{"name", "Bob", "time",
                    DateTimeValue.datetime( 1981, 5, 11, 7, 30, 0, 0, "-05:00" )}, labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldParseLocalTimePropertyValues() throws Exception
    {
        // GIVEN
        DataFactory data = data(
                ":ID,name,time:LocalTime\n" +
                "0,Mattias,13:37\n" +
                "1,Johan,\"16:20:01\"\n" );
        Iterable<DataFactory> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), datas(),
                defaultFormatRelationshipFileHeader(),
                IdType.ACTUAL, config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            assertNextNode( nodes, 0L, new Object[]{"name", "Mattias", "time",
                    LocalTimeValue.localTime( 13, 37, 0, 0 )}, labels() );
            assertNextNode( nodes, 1L, new Object[]{"name", "Johan", "time",
                    LocalTimeValue.localTime( 16, 20, 1, 0 )}, labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldParseLocalDateTimePropertyValues() throws Exception
    {
        // GIVEN
        DataFactory data = data(
                ":ID,name,time:LocalDateTime\n" +
                "0,Mattias,2018-02-27T13:37\n" +
                "1,Johan,\"2018-03-01T16:20:01\"\n" );
        Iterable<DataFactory> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), datas(),
                defaultFormatRelationshipFileHeader(),
                IdType.ACTUAL, config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            assertNextNode( nodes, 0L, new Object[]{"name", "Mattias", "time",
                    LocalDateTimeValue.localDateTime( 2018, 2, 27, 13, 37, 0, 0 )}, labels() );
            assertNextNode( nodes, 1L, new Object[]{"name", "Johan", "time",
                    LocalDateTimeValue.localDateTime( 2018, 3, 1, 16, 20, 1, 0 )}, labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldParseDurationPropertyValues() throws Exception
    {
        // GIVEN
        DataFactory data = data(
                ":ID,name,duration:Duration\n" +
                "0,Mattias,P3MT13H37M\n" +
                "1,Johan,\"P-1YT4H20M\"\n" );
        Iterable<DataFactory> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), datas(),
                defaultFormatRelationshipFileHeader(),
                IdType.ACTUAL, config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            assertNextNode( nodes, 0L, new Object[]{"name", "Mattias", "duration",
                    DurationValue.duration( 3, 0, 13 * 3600 + 37 * 60, 0 )}, labels() );
            assertNextNode( nodes, 1L, new Object[]{"name", "Johan", "duration",
                    DurationValue.duration( -12, 0, 4 * 3600 + 20 * 60, 0 )}, labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldFailOnArrayDelimiterBeingSameAsDelimiter()
    {
        // WHEN
        try
        {
            new CsvInput( null, null, null, null, IdType.ACTUAL, customConfig( ',', ',', '"' ),
                    silentBadCollector( 0 ) );
            fail( "Should not be possible" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN
            assertTrue( e.getMessage().contains( "array delimiter" ) );
        }
    }

    @Test
    public void shouldFailOnQuotationCharacterBeingSameAsDelimiter()
    {
        // WHEN
        try
        {
            new CsvInput( null, null, null, null, IdType.ACTUAL, customConfig( ',', ';', ',' ),
                    silentBadCollector( 0 ) );
            fail( "Should not be possible" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN
            assertTrue( e.getMessage().contains( "delimiter" ) );
            assertTrue( e.getMessage().contains( "quotation" ) );
        }
    }

    @Test
    public void shouldFailOnQuotationCharacterBeingSameAsArrayDelimiter()
    {
        // WHEN
        try
        {
            new CsvInput( null, null, null, null, IdType.ACTUAL, customConfig( ',', ';', ';' ),
                    silentBadCollector( 0 ) );
            fail( "Should not be possible" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN
            assertTrue( e.getMessage().contains( "array delimiter" ) );
            assertTrue( e.getMessage().contains( "quotation" ) );
        }
    }

    @Test
    public void shouldHaveNodesBelongToGroupSpecifiedInHeader() throws Exception
    {
        // GIVEN
        IdType idType = IdType.INTEGER;
        Iterable<DataFactory> data = dataIterable( data(
                "123,one\n" +
                "456,two" ) );
        Groups groups = new Groups();
        Group group = groups.getOrCreate( "MyGroup" );
        Input input = new CsvInput(
                data,
                header( entry( null, Type.ID, group.name(), idType.extractor( extractors ) ),
                        entry( "name", Type.PROPERTY, extractors.string() ) ),
                        datas(), defaultFormatRelationshipFileHeader(), idType, config( COMMAS ),
                        silentBadCollector( 0 ) );

        // WHEN/THEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            assertNextNode( nodes, group, 123L, properties( "name", "one" ), labels() );
            assertNextNode( nodes, group, 456L, properties( "name", "two" ), labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldHaveRelationshipsSpecifyStartEndNodeIdGroupsInHeader() throws Exception
    {
        // GIVEN
        IdType idType = IdType.INTEGER;
        Iterable<DataFactory> data = dataIterable( data(
                "123,TYPE,234\n" +
                "345,TYPE,456" ) );
        Groups groups = new Groups();
        Group startNodeGroup = groups.getOrCreate( "StartGroup" );
        Group endNodeGroup = groups.getOrCreate( "EndGroup" );
        Iterable<DataFactory> nodeHeader = dataIterable(
                data( ":ID(" + startNodeGroup.name() + ")" ),
                data( ":ID(" + endNodeGroup.name() + ")" ) );
        Input input = new CsvInput( nodeHeader, defaultFormatNodeFileHeader(),
                data,
                header( entry( null, Type.START_ID, startNodeGroup.name(), idType.extractor( extractors ) ),
                        entry( null, Type.TYPE, extractors.string() ),
                        entry( null, Type.END_ID, endNodeGroup.name(), idType.extractor( extractors ) ) ),
                        idType, config( COMMAS ),
                        silentBadCollector( 0 ) );

        // WHEN/THEN
        try ( InputIterator relationships = input.relationships().iterator() )
        {
            assertRelationship( relationships, startNodeGroup, 123L, endNodeGroup, 234L, "TYPE", properties() );
            assertRelationship( relationships, startNodeGroup, 345L, endNodeGroup, 456L, "TYPE", properties() );
            assertFalse( readNext( relationships ) );
        }
    }

    @Test
    public void shouldDoWithoutRelationshipTypeHeaderIfDefaultSupplied() throws Exception
    {
        // GIVEN relationship data w/o :TYPE header
        String defaultType = "HERE";
        DataFactory data = data(
                ":START_ID,:END_ID,name\n" +
                "0,1,First\n" +
                "2,3,Second\n", defaultRelationshipType( defaultType ) );
        Iterable<DataFactory> dataIterable = dataIterable( data );
        Input input = new CsvInput( datas(), defaultFormatNodeFileHeader(), dataIterable, defaultFormatRelationshipFileHeader(),
                IdType.ACTUAL, config( COMMAS ),
                silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator relationships = input.relationships().iterator() )
        {
            // THEN
            assertNextRelationship( relationships, 0L, 1L, defaultType, properties( "name", "First" ) );
            assertNextRelationship( relationships, 2L, 3L, defaultType, properties( "name", "Second" ) );
            assertFalse( readNext( relationships ) );
        }
    }

    @Test
    public void shouldIgnoreNodeEntriesMarkedIgnoreUsingHeader() throws Exception
    {
        // GIVEN
        Iterable<DataFactory> data = DataFactories.datas( CsvInputTest.data(
                ":ID,name:IGNORE,other:int,:LABEL\n" +
                "1,Mattias,10,Person\n" +
                "2,Johan,111,Person\n" +
                "3,Emil,12,Person" ) );
        Input input = new CsvInput( data, defaultFormatNodeFileHeader(), datas(), defaultFormatNodeFileHeader(), IdType.INTEGER,
                config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            assertNextNode( nodes, 1L, new Object[] {"other", 10}, labels( "Person" ) );
            assertNextNode( nodes, 2L, new Object[] {"other", 111}, labels( "Person" ) );
            assertNextNode( nodes, 3L, new Object[] {"other", 12}, labels( "Person" ) );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldIgnoreRelationshipEntriesMarkedIgnoreUsingHeader() throws Exception
    {
        // GIVEN
        Iterable<DataFactory> data = DataFactories.datas( CsvInputTest.data(
                ":START_ID,:TYPE,:END_ID,prop:IGNORE,other:int\n" +
                "1,KNOWS,2,Mattias,10\n" +
                "2,KNOWS,3,Johan,111\n" +
                "3,KNOWS,4,Emil,12" ) );
        Input input = new CsvInput( datas(), defaultFormatNodeFileHeader(), data, defaultFormatRelationshipFileHeader(), IdType.INTEGER,
                config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator relationships = input.relationships().iterator() )
        {
            assertNextRelationship( relationships, 1L, 2L, "KNOWS", new Object[] {"other", 10} );
            assertNextRelationship( relationships, 2L, 3L, "KNOWS", new Object[] {"other", 111} );
            assertNextRelationship( relationships, 3L, 4L, "KNOWS", new Object[] {"other", 12} );
            assertFalse( readNext( relationships ) );
        }
    }

    @Test
    public void shouldPropagateExceptionFromFailingDecorator() throws Exception
    {
        // GIVEN
        RuntimeException failure = new RuntimeException( "FAILURE" );
        Iterable<DataFactory> data =
                DataFactories.datas( CsvInputTest.data( ":ID,name\n1,Mattias",
                        new FailingNodeDecorator( failure ) ) );
        Input input = new CsvInput( data, defaultFormatNodeFileHeader(), datas(), defaultFormatNodeFileHeader(), IdType.INTEGER,
                config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            readNext( nodes );
        }
        catch ( InputException e )
        {
            // THEN
            assertTrue( e.getCause() == failure );
        }
    }

    @Test
    public void shouldNotIncludeEmptyArraysInEntities() throws Exception
    {
        // GIVEN
        Iterable<DataFactory> data = DataFactories.datas( CsvInputTest.data(
                ":ID,sprop:String[],lprop:long[]\n" +
                "1,,\n" +
                "2,a;b,10;20"
                ) );
        Input input = new CsvInput( data, defaultFormatNodeFileHeader(), datas(), defaultFormatNodeFileHeader(), IdType.INTEGER,
                config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN/THEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            assertNextNode( nodes, 1L, NO_PROPERTIES, labels() );
            assertNextNode( nodes, 2L, properties( "sprop", new String[] {"a", "b"}, "lprop", new long[] {10, 20} ),
                    labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldTreatEmptyQuotedStringsAsNullIfConfiguredTo() throws Exception
    {
        // GIVEN
        Iterable<DataFactory> data = DataFactories.datas( CsvInputTest.data(
                ":ID,one,two,three\n" +
                "1,\"\",,value" ) );
        Configuration config = config( new Configuration.Overridden( COMMAS )
        {
            @Override
            public boolean emptyQuotedStringsAsNull()
            {
                return true;
            }
        } );
        Input input = new CsvInput( data, defaultFormatNodeFileHeader(),
                datas(), defaultFormatRelationshipFileHeader(), IdType.INTEGER, config, silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            assertNextNode( nodes, 1L, properties( "three", "value" ), labels() );
            assertFalse( readNext( nodes ) );
        }
    }

    @Test
    public void shouldIgnoreEmptyExtraColumns() throws Exception
    {
        // GIVEN
        Iterable<DataFactory> data = DataFactories.datas( CsvInputTest.data(
                ":ID,one\n" +
                "1,test,\n" +
                "2,test,,additional" ) );

        // WHEN
        Collector collector = mock( Collector.class );
        Input input = new CsvInput( data, defaultFormatNodeFileHeader(),
                datas(), defaultFormatRelationshipFileHeader(), IdType.INTEGER, config( COMMAS ), collector );

        // THEN
        try ( InputIterator nodes = input.nodes().iterator() )
        {
            // THEN
            assertNextNode( nodes, 1L, properties( "one", "test" ), labels() );
            assertNextNode( nodes, 2L, properties( "one", "test" ), labels() );
            assertFalse( readNext( nodes ) );
        }
        verify( collector, times( 1 ) ).collectExtraColumns( anyString(), eq( 1L ), eq( null ) );
        verify( collector, times( 1 ) ).collectExtraColumns( anyString(), eq( 2L ), eq( null ) );
        verify( collector, times( 1 ) ).collectExtraColumns( anyString(), eq( 2L ), eq( "additional" ) );
    }

    @Test
    public void shouldSkipRelationshipValidationIfToldTo() throws Exception
    {
     // GIVEN
        Iterable<DataFactory> data = datas( CsvInputTest.data(
                ":START_ID,:END_ID,:TYPE\n" +
                ",," ) );
        Input input = new CsvInput( datas(), defaultFormatNodeFileHeader(), data, defaultFormatRelationshipFileHeader(), IdType.INTEGER,
                config( COMMAS ), silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator relationships = input.relationships().iterator() )
        {
            readNext( relationships );
            assertNull( visitor.startId() );
            assertNull( visitor.endId() );
            assertNull( visitor.stringType );
        }
    }

    @Test
    public void shouldFailOnUnparsableNodeHeader()
    {
        // given
        Iterable<DataFactory> data = datas( data( ":SOMETHING,abcde#rtg:123," ) );

        try
        {
            // when
            new CsvInput( data, defaultFormatNodeFileHeader(), datas(), defaultFormatRelationshipFileHeader(), IdType.INTEGER,
                    COMMAS, mock( Collector.class ) );
            fail( "Should not parse" );
        }
        catch ( InputException e )
        {
            // then
            // OK
        }
    }

    @Test
    public void shouldFailOnUnparsableRelationshipHeader()
    {
        // given
        Iterable<DataFactory> data = datas( data( ":SOMETHING,abcde#rtg:123," ) );

        try
        {
            // when
            new CsvInput( datas(), defaultFormatNodeFileHeader(), data, defaultFormatRelationshipFileHeader(), IdType.INTEGER,
                    COMMAS, mock( Collector.class ) );
            fail( "Should not parse" );
        }
        catch ( InputException e )
        {
            // then
            // OK
        }
    }

    @Test
    public void shouldFailOnUndefinedGroupInRelationshipHeader()
    {
        // given
        Iterable<DataFactory> nodeData = datas( data( ":ID(left)" ), data( ":ID(right)" ) );
        Iterable<DataFactory> relationshipData = datas( data( ":START_ID(left),:END_ID(rite)" ) );

        try
        {
            // when
            new CsvInput( nodeData, defaultFormatNodeFileHeader(), relationshipData, defaultFormatRelationshipFileHeader(), IdType.INTEGER,
                    COMMAS, mock( Collector.class ) );
            fail( "Should not validate" );
        }
        catch ( InputException e )
        {
            // then
            // OK
        }
    }

    @Test
    public void shouldFailOnGlobalGroupInRelationshipHeaderIfNoGLobalGroupInNodeHeader()
    {
        // given
        Iterable<DataFactory> nodeData = datas( data( ":ID(left)" ), data( ":ID(right)" ) );
        Iterable<DataFactory> relationshipData = datas( data( ":START_ID(left),:END_ID(rite)" ) );

        try
        {
            // when
            new CsvInput( nodeData, defaultFormatNodeFileHeader(), relationshipData, defaultFormatRelationshipFileHeader(), IdType.INTEGER,
                    COMMAS, mock( Collector.class ) );
            fail( "Should not validate" );
        }
        catch ( InputException e )
        {
            // then
            // OK
        }
    }

    private Configuration customConfig( final char delimiter, final char arrayDelimiter, final char quote )
    {
        return config( new Configuration.Default()
        {
            @Override
            public char quotationCharacter()
            {
                return quote;
            }

            @Override
            public char delimiter()
            {
                return delimiter;
            }

            @Override
            public char arrayDelimiter()
            {
                return arrayDelimiter;
            }
        } );
    }

    private DataFactory given( final CharReadable data )
    {
        return config -> dataItem( data, InputEntityDecorators.NO_DECORATOR );
    }

    private DataFactory data( final CharReadable data, final Decorator decorator )
    {
        return config -> dataItem( data, decorator );
    }

    private static Data dataItem( final CharReadable data, final Decorator decorator )
    {
        return DataFactories.data( decorator, () -> data ).create( COMMAS /*doesn't matter here in this test*/ );
    }

    private void assertNextRelationship( InputIterator relationship,
            Object startNode, Object endNode, String type, Object[] properties ) throws IOException
    {
        assertRelationship( relationship, GLOBAL, startNode, GLOBAL, endNode, type, properties );
    }

    private void assertRelationship( InputIterator data,
            Group startNodeGroup, Object startNode,
            Group endNodeGroup, Object endNode,
            String type, Object[] properties ) throws IOException
    {
        assertTrue( readNext( data ) );
        assertEquals( startNodeGroup, visitor.startIdGroup );
        assertEquals( startNode, visitor.startId() );
        assertEquals( endNodeGroup, visitor.endIdGroup );
        assertEquals( endNode, visitor.endId() );
        assertEquals( type, visitor.stringType );
        assertArrayEquals( properties, visitor.properties() );
    }

    private void assertNextNode( InputIterator data, Object id, Object[] properties, Set<String> labels )
            throws IOException
    {
        assertNextNode( data, GLOBAL, id, properties, labels );
    }

    private void assertNextNode( InputIterator data, Group group, Object id, Object[] properties, Set<String> labels )
            throws IOException
    {
        assertTrue( readNext( data ) );
        assertEquals( group.id(), visitor.idGroup.id() );
        assertEquals( id, visitor.id() );
        assertArrayEquals( properties, visitor.properties() );
        assertEquals( labels, asSet( visitor.labels() ) );
    }

    private boolean readNext( InputIterator data ) throws IOException
    {
        if ( referenceData != data )
        {
            chunk = null;
            referenceData = data;
        }

        if ( chunk == null )
        {
            chunk = data.newChunk();
            if ( !data.next( chunk ) )
            {
                return false;
            }
        }

        if ( chunk.next( visitor ) )
        {
            return true;
        }
        if ( !data.next( chunk ) )
        {
            return false;
        }
        return chunk.next( visitor );
    }

    private Object[] properties( Object... keysAndValues )
    {
        return keysAndValues;
    }

    private Set<String> labels( String... labels )
    {
        return asSet( labels );
    }

    private Header.Factory header( final Header.Entry... entries )
    {
        return new Header.Factory()
        {
            @Override
            public boolean isDefined()
            {
                return true;
            }

            @Override
            public Header create( CharSeeker dataSeeker, Configuration configuration, IdType idType, Groups groups )
            {
                return new Header( entries );
            }
        };
    }

    private Header.Entry entry( String name, Type type, Extractor<?> extractor )
    {
        return entry( name, type, null, extractor );
    }

    private Header.Entry entry( String name, Type type, String groupName, Extractor<?> extractor )
    {
        return new Header.Entry( name, type, groups.getOrCreate( groupName ), extractor );
    }

    private static DataFactory data( final String data )
    {
        return data( data, value -> value );
    }

    private static DataFactory data( final String data, final Decorator decorator )
    {
        return config -> dataItem( charReader( data ), decorator );
    }

    private static CharReadable charReader( String data )
    {
        return wrap( data );
    }

    private Iterable<DataFactory> dataIterable( DataFactory... data )
    {
        return Iterables.iterable( data );
    }

    private static class CapturingDataFactories implements Iterable<DataFactory>
    {
        private final Function<Configuration,CharReadable> factory;
        private CharReadable last;
        private final Decorator decorator;

        CapturingDataFactories( Function<Configuration,CharReadable> factory, Decorator decorator )
        {
            this.factory = factory;
            this.decorator = decorator;
        }

        @Override
        public Iterator<DataFactory> iterator()
        {
            return Iterators.iterator( config -> new Data()
            {
                @Override
                public RawIterator<CharReadable,IOException> stream()
                {
                    last = factory.apply( config );
                    return Readables.iterator( in -> in, last );
                }

                @Override
                public Decorator decorator()
                {
                    return decorator;
                }
            } );
        }

        CharReadable last()
        {
            return last;
        }
    }

    private static class FailingNodeDecorator implements Decorator
    {
        private final RuntimeException failure;

        FailingNodeDecorator( RuntimeException failure )
        {
            this.failure = failure;
        }

        @Override
        public InputEntityVisitor apply( InputEntityVisitor t )
        {
            return new InputEntityVisitor.Delegate( t )
            {
                @Override
                public void endOfEntity()
                {
                    throw failure;
                }
            };
        }
    }

    private Configuration config( Configuration config )
    {
        return new Configuration.Overridden( config )
        {
            @Override
            public boolean multilineFields()
            {
                return allowMultilineFields;
            }
        };
    }
}
