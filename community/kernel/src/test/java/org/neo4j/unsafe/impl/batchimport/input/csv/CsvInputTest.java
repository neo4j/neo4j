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
import java.util.Iterator;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;

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
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static org.neo4j.csv.reader.Readables.wrap;
import static org.neo4j.helpers.ArrayUtil.union;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntity.NO_PROPERTIES;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.additiveLabels;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.defaultRelationshipType;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatRelationshipFileHeader;

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
                        null, null, idType, COMMAS );

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
                        entry( "since", Type.PROPERTY, extractors.long_() ) ), idType, COMMAS );

        // WHEN/THEN
        Iterator<InputRelationship> relationships = input.relationships().iterator();
        assertRelationship( relationships.next(), 0L, "node1", "node2", "KNOWS", properties( "since", 1234567L ) );
        assertRelationship( relationships.next(), 1L, "node2", "node10", "HACKS", properties( "since", 987654L ) );
    }

    @Test
    public void shouldCloseDataIteratorsInTheEnd() throws Exception
    {
        // GIVEN
        CharSeeker nodeData = spy( charSeeker( "test" ) );
        CharSeeker relationshipData = spy( charSeeker( "test" ) );
        IdType idType = IdType.STRING;
        Iterable<DataFactory<InputNode>> nodeDataIterable = dataIterable( given( nodeData ) );
        Iterable<DataFactory<InputRelationship>> relationshipDataIterable = dataIterable( given( relationshipData ) );
        Input input = new CsvInput(
                nodeDataIterable, header( entry( "single", Type.IGNORE, idType.extractor( extractors ) ) ),
                relationshipDataIterable, header( entry( "single", Type.IGNORE, idType.extractor( extractors ) ) ),
                idType, COMMAS );

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
                null, null, IdType.ACTUAL, Configuration.COMMAS );

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
                null, null, IdType.ACTUAL, Configuration.COMMAS );

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
                                    IdType.STRING, Configuration.COMMAS );

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
                null, null, IdType.ACTUAL, Configuration.COMMAS );

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
                dataIterable, defaultFormatRelationshipFileHeader(), IdType.ACTUAL, Configuration.COMMAS );

        // WHEN/THEN
        try ( ResourceIterator<InputRelationship> relationships = input.relationships().iterator() )
        {
            assertRelationship( relationships.next(), 0L, 0L, 1L, defaultType, NO_PROPERTIES );
            assertRelationship( relationships.next(), 1L, 1L, 2L, customType, NO_PROPERTIES );
            assertRelationship( relationships.next(), 2L, 2L, 1L, defaultType, NO_PROPERTIES );
            assertFalse( relationships.hasNext() );
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
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), null, null, IdType.STRING, COMMAS );

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
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), null, null, IdType.STRING, COMMAS );

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
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), null, null, IdType.STRING, COMMAS );

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
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), null, null, IdType.STRING, COMMAS );

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
        Input input = new CsvInput( dataIterable, defaultFormatNodeFileHeader(), null, null, IdType.ACTUAL, COMMAS );

        // WHEN
        try ( ResourceIterator<InputNode> nodes = input.nodes().iterator() )
        {
            // THEN
            assertNode( nodes.next(), 0L, new Object[] {"name", "Mattias", "level", 1}, labels() );
            assertNode( nodes.next(), 1L, new Object[] {"name", "Johan", "level", 2}, labels() );
            assertFalse( nodes.hasNext() );
        }
    }

    private <ENTITY extends InputEntity> DataFactory<ENTITY> given( final CharSeeker data )
    {
        return new DataFactory<ENTITY>()
        {
            @Override
            public Data<ENTITY> create( Configuration config )
            {
                return noDecoratorData( data, Functions.<ENTITY>identity() );
            }
        };
    }

    private <ENTITY extends InputEntity> Data<ENTITY> noDecoratorData( final CharSeeker data,
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

    private void assertRelationship( InputRelationship relationship, long id, Object startNode, Object endNode,
            String type, Object[] properties )
    {
        assertEquals( id, relationship.id() );
        assertEquals( startNode, relationship.startNode() );
        assertEquals( endNode, relationship.endNode() );
        assertEquals( type, relationship.type() );
        assertArrayEquals( properties, relationship.properties() );
    }

    private void assertNode( InputNode node, Object id, Object[] properties, Set<String> labels )
    {
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
        return new Header.Entry( name, type, extractor );
    }

    private <ENTITY extends InputEntity> DataFactory<ENTITY> data( final String data )
    {
        return data( data, Functions.<ENTITY>identity() );
    }

    private <ENTITY extends InputEntity> DataFactory<ENTITY> data( final String data,
            final Function<ENTITY,ENTITY> decorator )
    {
        return new DataFactory<ENTITY>()
        {
            @Override
            public Data<ENTITY> create( Configuration config )
            {
                return noDecoratorData( charSeeker( data ), decorator );
            }
        };
    }

    private CharSeeker charSeeker( String data )
    {
        return new BufferedCharSeeker( wrap( new StringReader( data ) ) );
    }

    @SuppressWarnings( { "rawtypes", "unchecked" } )
    private <ENTITY extends InputEntity> Iterable<DataFactory<ENTITY>> dataIterable( DataFactory... data )
    {
        return Iterables.<DataFactory<ENTITY>,DataFactory<ENTITY>>iterable( data );
    }

    public final @Rule TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    private final Extractors extractors = new Extractors( ',' );
}
