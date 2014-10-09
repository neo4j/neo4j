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

import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputException;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.csv.reader.BufferedCharSeeker;
import org.neo4j.unsafe.impl.batchimport.input.csv.reader.CharSeeker;
import org.neo4j.unsafe.impl.batchimport.input.csv.reader.Extractor;
import org.neo4j.unsafe.impl.batchimport.input.csv.reader.Extractors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;

public class CsvInputTest
{
    @Test
    public void shouldProvideNodesFromCsvInput() throws Exception
    {
        // GIVEN
        IdType idType = IdType.ACTUAL;
        Input input = new CsvInput(
                data( "123,Mattias Persson,HACKER" ),
                header( entry( "id", Type.ID, idType.extractor() ),
                        entry( "name", Type.PROPERTY, Extractors.STRING ),
                        entry( "labels", Type.LABEL, Extractors.STRING ) ),
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
                data( "node1,node2,KNOWS,1234567\n" +
                      "node2,node10,HACKS,987654" ),
                header( entry( "from", Type.START_NODE, idType.extractor() ),
                        entry( "to", Type.END_NODE, idType.extractor() ),
                        entry( "type", Type.RELATIONSHIP_TYPE, Extractors.STRING ),
                        entry( "since", Type.PROPERTY, Extractors.LONG ) ), idType, COMMAS );

        // WHEN/THEN
        Iterator<InputRelationship> relationships = input.relationships().iterator();
        assertRelationship( relationships.next(), 0L, "node1", "node2", "KNOWS", properties( "since", 1234567L ) );
        assertRelationship( relationships.next(), 1L, "node2", "node10", "HACKS", properties( "since", 987654L ) );
    }

    @Test
    public void shouldCloseDataIteratorsInTheEnd() throws Exception
    {
        // GIVEN
        CharSeeker nodeData = mock( CharSeeker.class );
        CharSeeker relationshipData = mock( CharSeeker.class );
        IdType idType = IdType.STRING;
        Input input = new CsvInput(
                given( nodeData ), header( entry( "single", Type.IGNORE, idType.extractor() ) ),
                given( relationshipData ), header( entry( "single", Type.IGNORE, idType.extractor() ) ),
                idType, COMMAS );

        // WHEN
        input.nodes().iterator().close();
        input.relationships().iterator().close();

        // THEN
        verify( nodeData, times( 1 ) ).close();
        verify( relationshipData, times( 1 ) ).close();
    }

    @Test
    public void shouldFailForDataThatLacksEntries() throws Exception
    {
        // GIVEN
        IdType idType = IdType.ACTUAL;
        Input input = new CsvInput( data( "10" ),
                header( entry( "id", Type.ID, idType.extractor() ),
                        entry( "name", Type.PROPERTY, idType.extractor() ) ),
                null, null, idType, COMMAS );

        // WHEN/THEN
        Iterator<InputNode> nodes = input.nodes().iterator();
        try
        {
            nodes.next();
            fail( "Should fail" );
        }
        catch ( InputException e )
        {
            // Good
        }
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
            public Header create( CharSeeker from, Configuration configuration, Extractor<?> idExtractor )
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
                return new BufferedCharSeeker( new StringReader( data ) );
            }
        };
    }

    public final @Rule TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
}
