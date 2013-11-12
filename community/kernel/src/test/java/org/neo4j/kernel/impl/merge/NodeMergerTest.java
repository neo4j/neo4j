/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.merge;

import com.sun.xml.internal.xsom.impl.scd.Iterators;
import org.junit.Test;
import org.mockito.Mockito;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.coreapi.ThreadToStatementContextBridge;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.kernel.api.properties.Property.stringProperty;

public class NodeMergerTest
{
    private final ReadOperations readOps = mock( ReadOperations.class );
    private final DataWriteOperations dataOps = mock( DataWriteOperations.class );
    private final TokenWriteOperations tokenOps = mock( TokenWriteOperations.class );

    @Test
    public void shouldDeduplicateLabels() throws Exception
    {
        // given
        labelId( "Foo", 1 );
        labelId( "Bar", 2 );
        propertyKeyId( "key1", 100 );

        // when
        NodeMerger merger = createMerger( "Foo", "Bar", "Foo" );

        // then
        assertArrayEquals( new int[] { 1, 2 }, merger.labelIds );
    }

    @Test
    public void shouldRejectDuplicateProperties() throws Exception
    {
        // given
        labelId( "Foo", 1 );
        propertyKeyId( "key1", 100 );

        NodeMerger merger = createMerger( "Foo" ).withProperty( "key1", "value1" );

        // when
        try
        {
            merger.withProperty( "key1", "value2" );

            fail( "expected exception" );
        }
        // then
        catch ( IllegalArgumentException e )
        {
            assertEquals( "Multiple values given for property 'key1'.", e.getMessage() );
        }
    }

    @Test
    public void shouldAcceptLabelButNoProperties() throws Exception
    {
        // given
        NodeMerger merger = createMerger( "Foo" );

        // when
        merger.merge();
    }

    @Test
    public void shouldAcceptNoLabelsAndNoProperties() throws Exception
    {
        // given
        NodeMerger merger = createMerger();

        // when
        merger.merge();
    }

    @Test
    public void shouldAcceptMultipleLabelsAndProperties() throws Exception
    {
        // given
        labelId( "Foo", 1 );
        labelId( "Bar", 2 );
        propertyKeyId( "key1", 100 );
        propertyKeyId( "key2", 101 );

        // when
        NodeMerger merger = createMerger( "Foo", "Bar" )
                .withProperty( "key1", "value1" )
                .withProperty( "key2", "value2" );

        // then all is good
        assertArrayEquals( new int[] { 1, 2 }, merger.labelIds );
        assertArrayEquals( new DefinedProperty[]{ stringProperty(100, "value1"), stringProperty( 101, "value2" ) },
                merger.properties );

    }
    private NodeMerger createMerger( String... labels ) throws Exception
    {
        Statement statement = mock( Statement.class );
        when( statement.readOperations() ).thenReturn( readOps );
        when( statement.dataWriteOperations() ).thenReturn( dataOps );
        when( statement.tokenWriteOperations() ).thenReturn( tokenOps );

        ThreadToStatementContextBridge statementContextProvider = mock( ThreadToStatementContextBridge.class );
        when( statementContextProvider.instance() ).thenReturn( statement );
        NodeManager nodeManager = mock( NodeManager.class );
        when( nodeManager.newNodeProxyById( anyLong() ) ).then( Mockito.CALLS_REAL_METHODS );
        when( nodeManager.getAllNodes() ).thenReturn( Iterators.<Node>empty()  );
        return NodeMerger.createMerger( statementContextProvider, nodeManager, labels( labels ) );
    }

    private void propertyKeyId( String name, int id ) throws IllegalTokenNameException
    {
        when( tokenOps.propertyKeyGetOrCreateForName( name ) ).thenReturn( id );
    }

    private void labelId( String name, int id ) throws IllegalTokenNameException, TooManyLabelsException
    {
        when( tokenOps.labelGetOrCreateForName( name ) ).thenReturn( id );
    }

    private static Label[] labels( String... names )
    {
        Label[] labels = new Label[names.length];
        for ( int i = 0; i < labels.length; i++ )
        {
            labels[i] = label( names[i] );
        }
        return labels;
    }
}
