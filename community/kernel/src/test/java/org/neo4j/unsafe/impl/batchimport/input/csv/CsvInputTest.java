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

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.csv.reader.BufferedCharSeeker;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
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

import static org.neo4j.unsafe.impl.batchimport.input.InputEntity.NO_LABELS;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;

public class CsvInputTest
{
    @Test
    public void shouldProvideNodesFromCsvInput() throws Exception
    {
        // GIVEN
        IdType idType = IdType.ACTUAL;
        Input input = new CsvInput(
                dataIterable( data( "123,Mattias Persson,HACKER" ) ),
                header( entry( "id", Type.ID, idType.extractor( extractors ) ),
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
        Input input = new CsvInput( null, null,
                dataIterable( data( "node1,node2,KNOWS,1234567\n" +
                      "node2,node10,HACKS,987654" ) ),
                header( entry( "from", Type.START_NODE, idType.extractor( extractors ) ),
                        entry( "to", Type.END_NODE, idType.extractor( extractors ) ),
                        entry( "type", Type.RELATIONSHIP_TYPE, extractors.string() ),
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
        Input input = new CsvInput(
                dataIterable( given( nodeData ) ), header( entry( "single", Type.IGNORE, idType.extractor( extractors ) ) ),
                dataIterable( given( relationshipData ) ), header( entry( "single", Type.IGNORE, idType.extractor( extractors ) ) ),
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
        Input input = new CsvInput(
                dataIterable( data( "1,ultralisk,ZERG,10\n" +
                                "2,corruptor,ZERG\n" +
                                "3,mutalisk,ZERG,3" ) ),
                header(
                      entry( "id", Type.ID, extractors.long_() ),
                      entry( "unit", Type.PROPERTY, extractors.string() ),
                      entry( "type", Type.LABEL, extractors.string() ),
                      entry( "kills", Type.PROPERTY, extractors.int_() ) ),
                null, null, IdType.ACTUAL, Configuration.COMMAS );

        // WHEN
        try ( ResourceIterator<InputNode> nodes = input.nodes().iterator() )
        {
            // THEN
            assertNode( nodes.next(), 1L, new Object[] { "unit", "ultralisk", "kills", 10 }, new String[] { "ZERG" } );
            assertNode( nodes.next(), 2L, new Object[] { "unit", "corruptor" }, new String[] { "ZERG" } );
            assertNode( nodes.next(), 3L, new Object[] { "unit", "mutalisk", "kills", 3 }, new String[] { "ZERG" } );
            assertFalse( nodes.hasNext() );
        }
    }

    @Test
    public void shouldIgnoreValuesAfterHeaderEntries() throws Exception
    {
        // GIVEN
        Input input = new CsvInput(
                dataIterable( data( "1,zergling,bubble,bobble\n" +
                                "2,scv,pun,intended" ) ),
                header(
                      entry( "id", Type.ID, extractors.long_() ),
                      entry( "name", Type.PROPERTY, extractors.string() ) ),
                null, null, IdType.ACTUAL, Configuration.COMMAS );

        // WHEN
        try ( ResourceIterator<InputNode> nodes = input.nodes().iterator() )
        {
            // THEN
            assertNode( nodes.next(), 1L, new Object[] { "name", "zergling" }, InputEntity.NO_LABELS );
            assertNode( nodes.next(), 2L, new Object[] { "name", "scv" }, InputEntity.NO_LABELS );
            assertFalse( nodes.hasNext() );
        }
    }

    @Test
    public void shouldHandleMultipleInputGroups() throws Exception
    {
        // GIVEN multiple input groups, each with their own, specific, header
        DataFactory group1 = data( "id:ID,name,kills:int,health:int\n" +
                                   "1,Jim,10,100\n" +
                                   "2,Abathur,0,200\n" );
        DataFactory group2 = data( "id:ID,type\n" +
                                   "3,zergling\n" +
                                   "4,csv\n" );
        Input input = new CsvInput( dataIterable( group1, group2 ), defaultFormatNodeFileHeader(),
                                    null, null,
                                    IdType.STRING, Configuration.COMMAS );

        // WHEN iterating over them, THEN the expected data should come out
        ResourceIterator<InputNode> nodes = input.nodes().iterator();
        assertNode( nodes.next(), "1", properties( "name", "Jim", "kills", 10, "health", 100 ), NO_LABELS );
        assertNode( nodes.next(), "2", properties( "name", "Abathur", "kills", 0, "health", 200 ), NO_LABELS );
        assertNode( nodes.next(), "3", properties( "type", "zergling" ), NO_LABELS );
        assertNode( nodes.next(), "4", properties( "type", "csv" ), NO_LABELS );
        assertFalse( nodes.hasNext() );
    }

    private DataFactory given( final CharSeeker data )
    {
        return new DataFactory()
        {
            @Override
            public CharSeeker create( Configuration config )
            {
                return data;
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

    private void assertNode( InputNode node, Object id, Object[] properties, String[] labels )
    {
        assertEquals( id, node.id() );
        assertArrayEquals( properties, node.properties() );
        assertArrayEquals( labels, node.labels() );
    }

    private Object[] properties( Object... keysAndValues )
    {
        return keysAndValues;
    }

    private String[] labels( String... labels )
    {
        return labels;
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

    private DataFactory data( final String data )
    {
        return new DataFactory()
        {
            @Override
            public CharSeeker create( Configuration config )
            {
                return charSeeker( data );
            }
        };
    }

    private CharSeeker charSeeker( String data )
    {
        return new BufferedCharSeeker( new StringReader( data ) );
    }

    private Iterable<DataFactory> dataIterable( DataFactory... data )
    {
        return Iterables.<DataFactory,DataFactory>iterable( data );
    }

    public final @Rule TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    private final Extractors extractors = new Extractors( ',' );
}
