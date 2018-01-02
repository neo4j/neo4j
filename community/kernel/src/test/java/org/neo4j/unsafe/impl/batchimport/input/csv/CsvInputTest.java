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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import org.junit.Rule;
import org.junit.Test;

import java.io.StringReader;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.csv.reader.BufferedCharSeeker;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.function.Function;
import org.neo4j.function.Functions;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.DataException;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.InputException;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static org.neo4j.csv.reader.Readables.wrap;
import static org.neo4j.helpers.ArrayUtil.union;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.unsafe.impl.batchimport.input.Collectors.silentBadCollector;
import static org.neo4j.unsafe.impl.batchimport.input.Group.GLOBAL;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntity.NO_PROPERTIES;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.additiveLabels;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.defaultRelationshipType;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatRelationshipFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.relationshipData;

public class CsvInputTest
{
    @Test
    public void shouldProvideNodesFromCsvInput() throws Exception
    {
        // GIVEN
        IdType idType = IdType.ACTUAL;
        Iterable<DataFactory<InputNode>> data = dataIterable( data( "123,Mattias Persson,HACKER" ) );
        Input input = new CsvInput(
                data,
                header( entry( null, Type.ID, idType.extractor( extractors ) ),
                        entry( "name", Type.PROPERTY, extractors.string() ),
                        entry( "labels", Type.LABEL, extractors.string() ) ),
                        null, null, idType, COMMAS, silentBadCollector( 0 ) );

        // WHEN/THEN
        Iterator<InputNode> nodes = input.nodes().iterator();
        assertNode( nodes.next(), 123L, properties( "name", "Mattias Persson" ), labels( "HACKER" ) );
        assertFalse( nodes.hasNext() );
    }

    @Test
    public void shouldProvideRelationshipsFromCsvInput() throws Exception
    {
        // GIVEN
        IdType idType = IdType.STRING;
        Iterable<DataFactory<InputRelationship>> data = dataIterable( data(
              "node1,node2,KNOWS,1234567\n" +
              "node2,node10,HACKS,987654" ) );
        Input input = new CsvInput( null, null,
                data,
                header( entry( "from", Type.START_ID, idType.extractor( extractors ) ),
                        entry( "to", Type.END_ID, idType.extractor( extractors ) ),
                        entry( "type", Type.TYPE, extractors.string() ),
                        entry( "since", Type.PROPERTY, extractors.long_() ) ), idType, COMMAS,
                        silentBadCollector( 0 ) );

        // WHEN/THEN
        Iterator<InputRelationship> relationships = input.relationships().iterator();
        assertRelationship( relationships.next(), "node1", "node2", "KNOWS", properties( "since", 1234567L ) );
        assertRelationship( relationships.next(), "node2", "node10", "HACKS", properties( "since", 987654L ) );
    }

    @Test
    public void shouldCloseDataIteratorsInTheEnd() throws Exception
    {
        // GIVEN
        CharSeeker nodeData = spy( charSeeker( "1" ) );
        CharSeeker relationshipData = spy( charSeeker( "1,1" ) );
        IdType idType = IdType.STRING;
        Iterable<DataFactory<InputNode>> nodeDataIterable = dataIterable( given( nodeData ) );
        Iterable<DataFactory<InputRelationship>> relationshipDataIterable =
                dataIterable( data( relationshipData, defaultRelationshipType( "TYPE" ) ) );
        Input input = new CsvInput(
                nodeDataIterable, header(
                        entry( null, Type.ID, idType.extractor( extractors ) ) ),
                relationshipDataIterable, header(
                        entry( null, Type.START_ID, idType.extractor( extractors ) ),
                        entry( null, Type.END_ID, idType.extractor( extractors ) ) ),
                idType, COMMAS, silentBadCollector( 0 ) );

        // WHEN
        try ( ResourceIterator<InputNode> iterator = input.nodes().iterator() )
        {
            iterator.next();
        }
        try ( ResourceIterator<InputRelationship> iterator = input.relationships().iterator() )
        {
            iterator.next();
        }

        // THEN
        verify( nodeData, times( 1 ) ).close();
        verify( relationshipData, times( 1 ) ).close();
    }

    @Test
    public void shouldCopeWithLinesThatHasTooFewValuesButStillValidates() throws Exception
    {
        // GIVEN
        Iterable<DataFactory<InputNode>> data = dataIterable( data( "1,ultralisk,ZERG,10\n" +
                                                                    "2,corruptor,ZERG\n" +
                                                                    "3,mutalisk,ZERG,3" ) );
        Input input = new CsvInput(
                data,
                header(
                      entry( null, Type.ID, extractors.long_() ),
                      entry( "unit", Type.PROPERTY, extractors.string() ),
                      entry( "type", Type.LABEL, extractors.string() ),
                      entry( "kills", Type.PROPERTY, extractors.int_() ) ),
                null, null, IdType.ACTUAL, Configuration.COMMAS, silentBadCollector( 0 ) );

        // WHEN
        try ( ResourceIterator<InputNode> nodes = input.nodes().iterator() )
        {
            // THEN
            assertNode( nodes.next(), 1L, new Object[] { "unit", "ultralisk", "kills", 10 }, labels( "ZERG" ) );
            assertNode( nodes.next(), 2L, new Object[] { "unit", "corruptor" }, labels( "ZERG" ) );
            assertNode( nodes.next(), 3L, new Object[] { "unit", "mutalisk", "kills", 3 }, labels( "ZERG" ) );
            assertFalse( nodes.hasNext() );
        }
    }

    @Test
    public void shouldIgnoreValuesAfterHeaderEntries() throws Exception
    {
        // GIVEN
        Iterable<DataFactory<InputNode>> data = dataIterable( data( "1,zergling,bubble,bobble\n" +
                                                                    "2,scv,pun,intended" ) );
        Input input = new CsvInput(
                data,
                header(
                      entry( null, Type.ID, extractors.long_() ),
                      entry( "name", Type.PROPERTY, extractors.string() ) ),
                null, null, IdType.ACTUAL, Configuration.COMMAS, silentBadCollector( 4 ) );

        // WHEN
        try ( ResourceIterator<InputNode> nodes = input.nodes().iterator() )
        {
            // THEN
            assertNode( nodes.next(), 1L, new Object[] { "name", "zergling" }, labels() );
            assertNode( nodes.next(), 2L, new Object[] { "name", "scv" }, labels() );
            assertFalse( nodes.hasNext() );
        }
    }

    @Test
    public void shouldHandleMultipleInputGroups() throws Exception
    {
        // GIVEN multiple input groups, each with their own, specific, header
        DataFactory<InputNode> group1 = data( ":ID,name,kills:int,health:int\n" +
                                              "1,Jim,10,100\n" +
                                              "2,Abathur,0,200\n" );
        DataFactory<InputNode> group2 = data( ":ID,type\n" +
                                              "3,zergling\n" +
                                              "4,csv\n" );
        Iterable<DataFactory<InputNode>> data = dataIterable( group1, group2 );
        Input input = new CsvInput( data, defaultFormatNodeFileHeader(),
                                    null, null,
                                    IdType.STRING, Configuration.COMMAS, silentBadCollector( 0 ) );

        // WHEN iterating over them, THEN the expected data should come out
        ResourceIterator<InputNode> nodes = input.nodes().iterator();
        assertNode( nodes.next(), "1", properties( "name", "Jim", "kills", 10, "health", 100 ), labels() );
        assertNode( nodes.next(), "2", properties( "name", "Abathur", "kills", 0, "health", 200 ), labels() );
        assertNode( nodes.next(), "3", properties( "type", "zergling" ), labels() );
        assertNode( nodes.next(), "4", properties( "type", "csv" ), labels() );
        assertFalse( nodes.hasNext() );
    }

    @Test
    public void shouldProvideAdditiveLabels() throws Exception
    {
        // GIVEN
        String[] addedLabels = {"Two", "AddTwo"};
        DataFactory<InputNode> data = data( ":ID,name,:LABEL\n" +
                                            "0,First,\n" +
                                            "1,Second,One\n" +
                                            "2,Third,One;Two",
                                            additiveLabels( addedLabels ) );
        Iterable<DataFactory<InputNode>> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(),
                null, null, IdType.ACTUAL, Configuration.COMMAS, silentBadCollector( 0 ) );

        // WHEN/THEN
        try ( ResourceIterator<InputNode> nodes = input.nodes().iterator() )
        {
            assertNode( nodes.next(), 0L, properties( "name", "First" ),
                    labels( addedLabels ) );
            assertNode( nodes.next(), 1L, properties( "name", "Second" ),
                    labels( union( new String[] {"One"}, addedLabels ) ) );
            assertNode( nodes.next(), 2L, properties( "name", "Third" ),
                    labels( union( new String[] {"One"}, addedLabels ) ) );
            assertFalse( nodes.hasNext() );
        }
    }

    @Test
    public void shouldProvideDefaultRelationshipType() throws Exception
    {
        // GIVEN
        String defaultType = "DEFAULT";
        String customType = "CUSTOM";
        DataFactory<InputRelationship> data = data( ":START_ID,:END_ID,:TYPE\n" +
                                                    "0,1,\n" +
                                                    "1,2," + customType + "\n" +
                                                    "2,1," + defaultType,
                                                    defaultRelationshipType( defaultType ) );
        Iterable<DataFactory<InputRelationship>> dataIterable = dataIterable( data );
        Input input = new CsvInput( null, null,
                dataIterable, defaultFormatRelationshipFileHeader(), IdType.ACTUAL, Configuration.COMMAS,
                silentBadCollector( 0 ) );

        // WHEN/THEN
        try ( ResourceIterator<InputRelationship> relationships = input.relationships().iterator() )
        {
            assertRelationship( relationships.next(), 0L, 1L, defaultType, NO_PROPERTIES );
            assertRelationship( relationships.next(), 1L, 2L, customType, NO_PROPERTIES );
            assertRelationship( relationships.next(), 2L, 1L, defaultType, NO_PROPERTIES );
            assertFalse( relationships.hasNext() );
        }
    }

    @Test
    public void shouldFailOnMissingRelationshipType() throws Exception
    {
        // GIVEN
        String type = "CUSTOM";
        DataFactory<InputRelationship> data = data( ":START_ID,:END_ID,:TYPE\n" +
                "0,1," + type + "\n" +
                "1,2," );
        Iterable<DataFactory<InputRelationship>> dataIterable = dataIterable( data );
        Input input = new CsvInput( null, null,
                dataIterable, defaultFormatRelationshipFileHeader(), IdType.ACTUAL, Configuration.COMMAS,
                silentBadCollector( 0 ) );

        // WHEN/THEN
        try ( ResourceIterator<InputRelationship> relationships = input.relationships().iterator() )
        {
            assertRelationship( relationships.next(), 0L, 1L, type, NO_PROPERTIES );
            try
            {
                relationships.next();
                fail( "Should have failed" );
            }
            catch ( DataException e )
            {
                assertTrue( e.getMessage().contains( Type.TYPE.name() ) );
            }
        }
    }

    @Test
    public void shouldAllowNodesWithoutIdHeader() throws Exception
    {
        // GIVEN
        DataFactory<InputNode> data = data(
                "name:string,level:int\n" +
                "Mattias,1\n" +
                "Johan,2\n" );
        Iterable<DataFactory<InputNode>> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), null, null, IdType.STRING, COMMAS,
                silentBadCollector( 0 ) );

        // WHEN
        try ( ResourceIterator<InputNode> nodes = input.nodes().iterator() )
        {
            // THEN
            assertNode( nodes.next(), null, new Object[] {"name", "Mattias", "level", 1}, labels() );
            assertNode( nodes.next(), null, new Object[] {"name", "Johan", "level", 2}, labels() );
            assertFalse( nodes.hasNext() );
        }
    }

    @Test
    public void shouldAllowSomeNodesToBeAnonymous() throws Exception
    {
        // GIVEN
        DataFactory<InputNode> data = data(
                ":ID,name:string,level:int\n" +
                "abc,Mattias,1\n" +
                ",Johan,2\n" ); // this node is anonymous
        Iterable<DataFactory<InputNode>> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), null, null, IdType.STRING, COMMAS,
                silentBadCollector( 0 ) );

        // WHEN
        try ( ResourceIterator<InputNode> nodes = input.nodes().iterator() )
        {
            // THEN
            assertNode( nodes.next(), "abc", new Object[] {"name", "Mattias", "level", 1}, labels() );
            assertNode( nodes.next(), null, new Object[] {"name", "Johan", "level", 2}, labels() );
            assertFalse( nodes.hasNext() );
        }
    }

    @Test
    public void shouldAllowNodesToBeAnonymousEvenIfIdHeaderIsNamed() throws Exception
    {
        // GIVEN
        DataFactory<InputNode> data = data(
                "id:ID,name:string,level:int\n" +
                "abc,Mattias,1\n" +
                ",Johan,2\n" ); // this node is anonymous
        Iterable<DataFactory<InputNode>> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), null, null, IdType.STRING, COMMAS,
                silentBadCollector( 0 ) );

        // WHEN
        try ( ResourceIterator<InputNode> nodes = input.nodes().iterator() )
        {
            // THEN
            assertNode( nodes.next(), "abc", new Object[] {"id", "abc", "name", "Mattias", "level", 1}, labels() );
            assertNode( nodes.next(), null, new Object[] {"name", "Johan", "level", 2}, labels() );
            assertFalse( nodes.hasNext() );
        }
    }

    @Test
    public void shouldHaveIdSetAsPropertyIfIdHeaderEntryIsNamed() throws Exception
    {
        // GIVEN
        DataFactory<InputNode> data = data(
                "myId:ID,name:string,level:int\n" +
                "abc,Mattias,1\n" +
                "def,Johan,2\n" ); // this node is anonymous
        Iterable<DataFactory<InputNode>> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), null, null, IdType.STRING, COMMAS,
                silentBadCollector( 0 ) );

        // WHEN
        try ( ResourceIterator<InputNode> nodes = input.nodes().iterator() )
        {
            // THEN
            assertNode( nodes.next(), "abc", new Object[] {"myId", "abc", "name", "Mattias", "level", 1}, labels() );
            assertNode( nodes.next(), "def", new Object[] {"myId", "def", "name", "Johan", "level", 2}, labels() );
            assertFalse( nodes.hasNext() );
        }
    }

    @Test
    public void shouldNotHaveIdSetAsPropertyIfIdHeaderEntryIsNamedForActualIds() throws Exception
    {
        // GIVEN
        DataFactory<InputNode> data = data(
                "myId:ID,name:string,level:int\n" +
                "0,Mattias,1\n" +
                "1,Johan,2\n" ); // this node is anonymous
        Iterable<DataFactory<InputNode>> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), null, null, IdType.ACTUAL, COMMAS,
                silentBadCollector( 0 ) );

        // WHEN
        try ( ResourceIterator<InputNode> nodes = input.nodes().iterator() )
        {
            // THEN
            assertNode( nodes.next(), 0L, new Object[] {"name", "Mattias", "level", 1}, labels() );
            assertNode( nodes.next(), 1L, new Object[] {"name", "Johan", "level", 2}, labels() );
            assertFalse( nodes.hasNext() );
        }
    }

    @Test
    public void shouldIgnoreEmptyPropertyValues() throws Exception
    {
        // GIVEN
        DataFactory<InputNode> data = data(
                ":ID,name,extra\n" +
                "0,Mattias,\n" +            // here we leave out "extra" property
                "1,Johan,Additional\n" );
        Iterable<DataFactory<InputNode>> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), null, null, IdType.ACTUAL, COMMAS,
                silentBadCollector( 0 ) );

        // WHEN
        try ( ResourceIterator<InputNode> nodes = input.nodes().iterator() )
        {
            // THEN
            assertNode( nodes.next(), 0L, new Object[] {"name", "Mattias"}, labels() );
            assertNode( nodes.next(), 1L, new Object[] {"name", "Johan", "extra", "Additional"}, labels() );
            assertFalse( nodes.hasNext() );
        }
    }

    @Test
    public void shouldIgnoreEmptyIntPropertyValues() throws Exception
    {
        // GIVEN
        DataFactory<InputNode> data = data(
                ":ID,name,extra:int\n" +
                "0,Mattias,\n" +            // here we leave out "extra" property
                "1,Johan,10\n" );
        Iterable<DataFactory<InputNode>> dataIterable = dataIterable( data );
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), null, null, IdType.ACTUAL, COMMAS,
                silentBadCollector( 0 ) );

        // WHEN
        try ( ResourceIterator<InputNode> nodes = input.nodes().iterator() )
        {
            // THEN
            assertNode( nodes.next(), 0L, new Object[] {"name", "Mattias"}, labels() );
            assertNode( nodes.next(), 1L, new Object[] {"name", "Johan", "extra", 10}, labels() );
            assertFalse( nodes.hasNext() );
        }
    }

    @Test
    public void shouldFailOnArrayDelimiterBeingSameAsDelimiter() throws Exception
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
    public void shouldFailOnQuotationCharacterBeingSameAsDelimiter() throws Exception
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
    public void shouldFailOnQuotationCharacterBeingSameAsArrayDelimiter() throws Exception
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
        IdType idType = IdType.ACTUAL;
        Iterable<DataFactory<InputNode>> data = dataIterable( data(
                "123,one\n" +
                "456,two" ) );
        Groups groups = new Groups();
        Group group = groups.getOrCreate( "MyGroup" );
        Input input = new CsvInput(
                data,
                header( entry( null, Type.ID, group.name(), idType.extractor( extractors ) ),
                        entry( "name", Type.PROPERTY, extractors.string() ) ),
                        null, null, idType, COMMAS,
                        silentBadCollector( 0 ) );

        // WHEN/THEN
        Iterator<InputNode> nodes = input.nodes().iterator();
        assertNode( nodes.next(), group, 123L, properties( "name", "one" ), labels() );
        assertNode( nodes.next(), group, 456L, properties( "name", "two" ), labels() );
        assertFalse( nodes.hasNext() );
    }

    @Test
    public void shouldHaveRelationshipsSpecifyStartEndNodeIdGroupsInHeader() throws Exception
    {
        // GIVEN
        IdType idType = IdType.ACTUAL;
        Iterable<DataFactory<InputRelationship>> data = dataIterable( data(
                "123,TYPE,234\n" +
                "345,TYPE,456" ) );
        Groups groups = new Groups();
        Group startNodeGroup = groups.getOrCreate( "StartGroup" );
        Group endNodeGroup = groups.getOrCreate( "EndGroup" );
        Input input = new CsvInput( null, null,
                data,
                header( entry( null, Type.START_ID, startNodeGroup.name(), idType.extractor( extractors ) ),
                        entry( null, Type.TYPE, extractors.string() ),
                        entry( null, Type.END_ID, endNodeGroup.name(), idType.extractor( extractors ) ) ),
                        idType, COMMAS,
                        silentBadCollector( 0 ) );

        // WHEN/THEN
        Iterator<InputRelationship> relationships = input.relationships().iterator();
        assertRelationship( relationships.next(), startNodeGroup, 123L, endNodeGroup, 234L, "TYPE", properties() );
        assertRelationship( relationships.next(), startNodeGroup, 345L, endNodeGroup, 456L, "TYPE", properties() );
        assertFalse( relationships.hasNext() );
    }

    @Test
    public void shouldDoWithoutRelationshipTypeHeaderIfDefaultSupplied() throws Exception
    {
        // GIVEN relationship data w/o :TYPE header
        String defaultType = "HERE";
        DataFactory<InputRelationship> data = data(
                ":START_ID,:END_ID,name\n" +
                "0,1,First\n" +
                "2,3,Second\n", defaultRelationshipType( defaultType ) );
        Iterable<DataFactory<InputRelationship>> dataIterable = dataIterable( data );
        Input input = new CsvInput( null, null, dataIterable, defaultFormatRelationshipFileHeader(),
                IdType.ACTUAL, COMMAS,
                silentBadCollector( 0 ) );

        // WHEN
        try ( ResourceIterator<InputRelationship> relationships = input.relationships().iterator() )
        {
            // THEN
            assertRelationship( relationships.next(), 0L, 1L, defaultType, properties( "name", "First" ) );
            assertRelationship( relationships.next(), 2L, 3L, defaultType, properties( "name", "Second" ) );
            assertFalse( relationships.hasNext() );
        }
    }

    @Test
    public void shouldIncludeDataSourceInformationOnBadFieldValueOrLine() throws Exception
    {
        // GIVEN
        Iterable<DataFactory<InputNode>> data = DataFactories.nodeData( CsvInputTest.<InputNode>data(
                ":ID,name,other:int\n" +
                "1,Mattias,10\n" +
                "2,Johan,abc\n" +
                "3,Emil,12" ) );
        Input input = new CsvInput( data, DataFactories.defaultFormatNodeFileHeader(), null, null,
                IdType.INTEGER, Configuration.COMMAS,
                silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator<InputNode> nodes = input.nodes().iterator() )
        {
            assertNode( nodes.next(), 1L, new Object[] {"name", "Mattias", "other", 10}, labels() );
            try
            {
                nodes.next();
                fail( "Should have failed" );
            }
            catch ( InputException e )
            {
                // THEN
                assertThat( e.getMessage(), containsString( "other" ) );
                assertThat( e.getMessage(), containsString( "abc" ) );
            }
        }
    }

    @Test
    public void shouldIgnoreNodeEntriesMarkedIgnoreUsingHeader() throws Exception
    {
        // GIVEN
        Iterable<DataFactory<InputNode>> data = DataFactories.nodeData( CsvInputTest.<InputNode>data(
                ":ID,name:IGNORE,other:int,:LABEL\n" +
                "1,Mattias,10,Person\n" +
                "2,Johan,111,Person\n" +
                "3,Emil,12,Person" ) );
        Input input = new CsvInput( data, defaultFormatNodeFileHeader(), null, null, IdType.INTEGER, COMMAS,
                silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator<InputNode> nodes = input.nodes().iterator() )
        {
            assertNode( nodes.next(), 1L, new Object[] {"other", 10}, labels( "Person" ) );
            assertNode( nodes.next(), 2L, new Object[] {"other", 111}, labels( "Person" ) );
            assertNode( nodes.next(), 3L, new Object[] {"other", 12}, labels( "Person" ) );
            assertFalse( nodes.hasNext() );
        }
    }

    @Test
    public void shouldIgnoreRelationshipEntriesMarkedIgnoreUsingHeader() throws Exception
    {
        // GIVEN
        Iterable<DataFactory<InputRelationship>> data = DataFactories.relationshipData( CsvInputTest.<InputRelationship>data(
                ":START_ID,:TYPE,:END_ID,prop:IGNORE,other:int\n" +
                "1,KNOWS,2,Mattias,10\n" +
                "2,KNOWS,3,Johan,111\n" +
                "3,KNOWS,4,Emil,12" ) );
        Input input = new CsvInput( null, null, data, defaultFormatRelationshipFileHeader(), IdType.INTEGER, COMMAS,
                silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator<InputRelationship> relationships = input.relationships().iterator() )
        {
            assertRelationship( relationships.next(), 1L, 2L, "KNOWS", new Object[] {"other", 10} );
            assertRelationship( relationships.next(), 2L, 3L, "KNOWS", new Object[] {"other", 111} );
            assertRelationship( relationships.next(), 3L, 4L, "KNOWS", new Object[] {"other", 12} );
            assertFalse( relationships.hasNext() );
        }
    }

    @Test
    public void shouldPropagateExceptionFromFailingDecorator() throws Exception
    {
        // GIVEN
        RuntimeException failure = new RuntimeException( "FAILURE" );
        Iterable<DataFactory<InputNode>> data =
                DataFactories.nodeData( CsvInputTest.<InputNode>data( ":ID,name\n1,Mattias",
                        new FailingNodeDecorator( failure ) ) );
        Input input = new CsvInput( data, defaultFormatNodeFileHeader(), null, null, IdType.INTEGER, COMMAS,
                silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator<InputNode> nodes = input.nodes().iterator() )
        {
            nodes.next();
        }
        catch ( RuntimeException e )
        {
            // THEN
            assertTrue( e == failure );
        }
    }

    @Test
    public void shouldNotIncludeEmptyArraysInEntities() throws Exception
    {
        // GIVEN
        Iterable<DataFactory<InputNode>> data = DataFactories.nodeData( CsvInputTest.<InputNode>data(
                ":ID,sprop:String[],lprop:long[]\n" +
                "1,,\n" +
                "2,a;b,10;20"
                ) );
        Input input = new CsvInput( data, defaultFormatNodeFileHeader(), null, null, IdType.INTEGER, COMMAS,
                silentBadCollector( 0 ) );

        // WHEN/THEN
        try ( InputIterator<InputNode> nodes = input.nodes().iterator() )
        {
            assertNode( nodes.next(), 1L, NO_PROPERTIES, labels() );
            assertNode( nodes.next(), 2L, properties( "sprop", new String[] {"a", "b"}, "lprop", new long[] {10, 20} ),
                    labels() );
            assertFalse( nodes.hasNext() );
        }
    }

    @Test
    public void shouldFailOnRelationshipWithMissingStartIdField() throws Exception
    {
        // GIVEN
        Iterable<DataFactory<InputRelationship>> data = relationshipData( CsvInputTest.<InputRelationship>data(
                ":START_ID,:END_ID,:TYPE\n" +
                ",1," ) );
        Input input = new CsvInput( null, null, data, defaultFormatRelationshipFileHeader(), IdType.INTEGER, COMMAS,
                silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator<InputRelationship> relationships = input.relationships().iterator() )
        {
            relationships.next();
            fail( "Should have failed" );
        }
        catch ( InputException e )
        {
            // THEN good
            assertThat( e.getMessage(), containsString( Type.START_ID.name() ) );
        }
    }

    @Test
    public void shouldFailOnRelationshipWithMissingEndIdField() throws Exception
    {
        // GIVEN
        Iterable<DataFactory<InputRelationship>> data = relationshipData( CsvInputTest.<InputRelationship>data(
                ":START_ID,:END_ID,:TYPE\n" +
                "1,," ) );
        Input input = new CsvInput( null, null, data, defaultFormatRelationshipFileHeader(), IdType.INTEGER, COMMAS,
                silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator<InputRelationship> relationships = input.relationships().iterator() )
        {
            relationships.next();
            fail( "Should have failed" );
        }
        catch ( InputException e )
        {
            // THEN good
            assertThat( e.getMessage(), containsString( Type.END_ID.name() ) );
        }
    }

    @Test
    public void shouldTreatEmptyQuotedStringsAsNullIfConfiguredTo() throws Exception
    {
        // GIVEN
        Iterable<DataFactory<InputNode>> data = DataFactories.nodeData( CsvInputTest.<InputNode>data(
                ":ID,one,two,three\n" +
                "1,\"\",,value" ) );
        Configuration config = new Configuration.Overriden( COMMAS )
        {
            @Override
            public boolean emptyQuotedStringsAsNull()
            {
                return true;
            }
        };
        Input input = new CsvInput( data, defaultFormatNodeFileHeader(),
                null, null, IdType.INTEGER, config, silentBadCollector( 0 ) );

        // WHEN
        try ( InputIterator<InputNode> nodes = input.nodes().iterator() )
        {
            InputNode node = nodes.next();
            // THEN
            assertNode( node, 1L, properties( "three", "value" ), labels() );
            assertFalse( nodes.hasNext() );
        }
    }

    @Test
    public void shouldIgnoreEmptyExtraColumns() throws Exception
    {
        // GIVEN
        Iterable<DataFactory<InputNode>> data = DataFactories.nodeData( CsvInputTest.<InputNode>data(
                ":ID,one\n" +
                "1,test,\n" +
                "2,test,,additional" ) );

        // WHEN
        Collector collector = mock( Collector.class );
        Input input = new CsvInput( data, defaultFormatNodeFileHeader(),
                null, null, IdType.INTEGER, COMMAS, collector );

        // THEN
        try ( InputIterator<InputNode> nodes = input.nodes().iterator() )
        {
            // THEN
            assertNode( nodes.next(), 1L, properties( "one", "test" ), labels() );
            assertNode( nodes.next(), 2L, properties( "one", "test" ), labels() );
            assertFalse( nodes.hasNext() );
        }
        verify( collector, times( 1 ) ).collectExtraColumns( anyString(), eq( 1l ), eq( (String)null ) );
        verify( collector, times( 1 ) ).collectExtraColumns( anyString(), eq( 2l ), eq( (String)null ) );
        verify( collector, times( 1 ) ).collectExtraColumns( anyString(), eq( 2l ), eq( "additional" ) );
    }

    private Configuration customConfig( final char delimiter, final char arrayDelimiter, final char quote )
    {
        return new Configuration.Default()
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
        };
    }

    private <ENTITY extends InputEntity> DataFactory<ENTITY> given( final CharSeeker data )
    {
        return new DataFactory<ENTITY>()
        {
            @Override
            public Data<ENTITY> create( Configuration config )
            {
                return dataItem( data, Functions.<ENTITY>identity() );
            }
        };
    }

    private <ENTITY extends InputEntity> DataFactory<ENTITY> data( final CharSeeker data,
            final Function<ENTITY,ENTITY> decorator )
    {
        return new DataFactory<ENTITY>()
        {
            @Override
            public Data<ENTITY> create( Configuration config )
            {
                return dataItem( data, decorator );
            }
        };
    }

    private static <ENTITY extends InputEntity> Data<ENTITY> dataItem( final CharSeeker data,
            final Function<ENTITY,ENTITY> decorator )
    {
        return new Data<ENTITY>()
        {
            @Override
            public CharSeeker stream()
            {
                return data;
            }

            @Override
            public Function<ENTITY,ENTITY> decorator()
            {
                return decorator;
            }
        };
    }

    private void assertRelationship( InputRelationship relationship,
            Object startNode, Object endNode, String type, Object[] properties )
    {
        assertRelationship( relationship, GLOBAL, startNode, GLOBAL, endNode, type, properties );
    }

    private void assertRelationship( InputRelationship relationship,
            Group startNodeGroup, Object startNode,
            Group endNodeGroup, Object endNode,
            String type, Object[] properties )
    {
        assertFalse( relationship.hasSpecificId() );
        assertEquals( startNodeGroup, relationship.startNodeGroup() );
        assertEquals( startNode, relationship.startNode() );
        assertEquals( endNodeGroup.id(), relationship.endNodeGroup().id() );
        assertEquals( endNode, relationship.endNode() );
        assertEquals( type, relationship.type() );
        assertArrayEquals( properties, relationship.properties() );
    }

    private void assertNode( InputNode node, Object id, Object[] properties, Set<String> labels )
    {
        assertNode( node, GLOBAL, id, properties, labels );
    }

    private void assertNode( InputNode node, Group group, Object id, Object[] properties, Set<String> labels )
    {
        assertEquals( group.id(), node.group().id() );
        assertEquals( id, node.id() );
        assertArrayEquals( properties, node.properties() );
        assertEquals( labels, asSet( node.labels() ) );
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
            public Header create( CharSeeker from, Configuration configuration, IdType idType )
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
        return new Header.Entry( name, type, groupName, extractor );
    }

    private static <ENTITY extends InputEntity> DataFactory<ENTITY> data( final String data )
    {
        return data( data, Functions.<ENTITY>identity() );
    }

    private static <ENTITY extends InputEntity> DataFactory<ENTITY> data( final String data,
            final Function<ENTITY,ENTITY> decorator )
    {
        return new DataFactory<ENTITY>()
        {
            @Override
            public Data<ENTITY> create( Configuration config )
            {
                return dataItem( charSeeker( data ), decorator );
            }
        };
    }

    private static final org.neo4j.csv.reader.Configuration SEEKER_CONFIG =
            new org.neo4j.csv.reader.Configuration.Overridden( new org.neo4j.csv.reader.Configuration.Default() )
    {
        @Override
        public int bufferSize()
        {
            return 1_000;
        }
    };

    private static CharSeeker charSeeker( String data )
    {
        return new BufferedCharSeeker( wrap( new StringReader( data ) ), SEEKER_CONFIG );
    }

    @SuppressWarnings( { "rawtypes", "unchecked" } )
    private <ENTITY extends InputEntity> Iterable<DataFactory<ENTITY>> dataIterable( DataFactory... data )
    {
        return Iterables.<DataFactory<ENTITY>,DataFactory<ENTITY>>iterable( data );
    }

    private static class FailingNodeDecorator implements Function<InputNode,InputNode>
    {
        private final RuntimeException failure;

        FailingNodeDecorator( RuntimeException failure )
        {
            this.failure = failure;
        }

        @Override
        public InputNode apply( InputNode from ) throws RuntimeException
        {
            throw failure;
        }
    }

    public final @Rule TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    private final Extractors extractors = new Extractors( ',' );
}
