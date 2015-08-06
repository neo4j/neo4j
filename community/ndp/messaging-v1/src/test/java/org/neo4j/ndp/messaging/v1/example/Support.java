/*
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
package org.neo4j.ndp.messaging.v1.example;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

import static java.util.Arrays.asList;

public class Support
{
    public static final List<Label> NO_LABELS = new ArrayList<>();
    public static final Map<String, Object> NO_PROPERTIES = new HashMap<>();

    // Collect labels from a Node
    public static Collection<Label> labels( Node node )
    {
        List<Label> labels = new ArrayList<>();
        for ( Label label : node.getLabels() )
        {
            labels.add( label );
        }
        return labels;
    }

    // Collect properties from a PropertyContainer
    public static Map<String, Object> properties( PropertyContainer entity )
    {
        Map<String, Object> properties = new HashMap<>();
        for ( String key : entity.getPropertyKeys() )
        {
            properties.put( key, entity.getProperty( key ) );
        }
        return properties;
    }

    // Helper to produce literal list of nodes
    public static List<Node> nodes( Node... nodes )
    {
        return new ArrayList<>( asList( nodes ) );
    }

    // Helper to extract list of nodes from a path
    public static List<Node> nodes( Path path )
    {
        List<Node> nodes = new ArrayList<>( path.length() + 1 );
        for ( Node node : path.nodes() )
        {
            nodes.add( node );
        }
        return nodes;
    }

    // Helper to produce literal list of relationships
    public static List<Relationship> relationships( Relationship... relationships )
    {
        return new ArrayList<>( asList( relationships ) );
    }

    // Helper to extract list of relationships from a path
    public static List<Relationship> relationships( Path path )
    {
        List<Relationship> relationships = new ArrayList<>( path.length() );
        for ( Relationship relationship : path.relationships() )
        {
            relationships.add( relationship );
        }
        return relationships;
    }

    // Helper to produce literal list of integers, used for path sequence information
    public static List<Integer> sequence( Integer... integers )
    {
        return new ArrayList<>( asList( integers ) );
    }

}
