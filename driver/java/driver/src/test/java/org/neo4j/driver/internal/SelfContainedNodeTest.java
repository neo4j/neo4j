/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.internal;

import org.junit.Test;

import java.util.List;

import org.neo4j.driver.Node;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.driver.Values;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.neo4j.driver.Values.properties;

public class SelfContainedNodeTest
{

    private Node adamTheNode()
    {
        return new SimpleNode( "A", asList( "Person" ),
                properties( "name", Values.value( "Adam" ) ) );
    }

    @Test
    public void testIdentity()
    {
        // Given
        Node node = adamTheNode();

        // Then
        assertThat( node.identity(), equalTo( Identities.identity( "A" ) ) );
    }

    @Test
    public void testLabels()
    {
        // Given
        Node node = adamTheNode();

        // Then
        List<String> labels = Iterables.toList( node.labels() );
        assertThat( labels.size(), equalTo( 1 ) );
        assertThat( labels.contains( "Person" ), equalTo( true ) );
    }

    @Test
    public void testKeys()
    {
        // Given
        Node node = adamTheNode();

        // Then
        List<String> keys = Iterables.toList( node.propertyKeys() );
        assertThat( keys.size(), equalTo( 1 ) );
        assertThat( keys.contains( "name" ), equalTo( true ) );
    }

    @Test
    public void testValue()
    {
        // Given
        Node node = adamTheNode();

        // Then
        assertThat( node.property( "name" ).javaString(), equalTo( "Adam" ) );
    }
}