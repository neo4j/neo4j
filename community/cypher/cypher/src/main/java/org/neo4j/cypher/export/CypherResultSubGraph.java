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
package org.neo4j.cypher.export;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;

import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.loop;

public class CypherResultSubGraph implements SubGraph
{

    private final SortedMap<Long, Node> nodes = new TreeMap<>();
    private final SortedMap<Long, Relationship> relationships = new TreeMap<>();
    private final Collection<Label> labels = new HashSet<>();
    private final Collection<IndexDefinition> indexes = new HashSet<>();
    private final Collection<ConstraintDefinition> constraints = new HashSet<>();

    public void add( Node node )
    {
        final long id = node.getId();
        if ( !nodes.containsKey( id ) )
        {
            addNode( id, node );
        }
    }

    void addNode( long id, Node data )
    {
        nodes.put( id, data );
        labels.addAll( asCollection( data.getLabels() ) );
    }

    public void add( Relationship rel )
    {
        final long id = rel.getId();
        if ( !relationships.containsKey( id ) )
        {
            addRel( id, rel );
            add( rel.getStartNode() );
            add( rel.getEndNode() );
        }
    }

    public static SubGraph from( Result result, GraphDatabaseService gds, boolean addBetween )
    {
        final CypherResultSubGraph graph = new CypherResultSubGraph();
        final List<String> columns = result.columns();
        for ( Map<String, Object> row : loop( result ) )
        {
            for ( String column : columns )
            {
                final Object value = row.get( column );
                graph.addToGraph( value );
            }
        }
        for ( IndexDefinition def : gds.schema().getIndexes() )
        {
            if ( graph.getLabels().contains( def.getLabel() ) )
            {
                graph.addIndex( def );
            }
        }
        for ( ConstraintDefinition def : gds.schema().getConstraints() )
        {
            if ( graph.getLabels().contains( def.getLabel() ) )
            {
                graph.addConstraint( def );
            }
        }
        if ( addBetween )
        {
            graph.addRelationshipsBetweenNodes();
        }
        return graph;
    }

    private void addIndex( IndexDefinition def )
    {
        indexes.add( def );
    }

    private void addConstraint( ConstraintDefinition def )
    {
        constraints.add( def );
    }

    private void addRelationshipsBetweenNodes()
    {
        Set<Node> newNodes = new HashSet<>();
        for ( Node node : nodes.values() )
        {
            for ( Relationship relationship : node.getRelationships() )
            {
                if ( !relationships.containsKey( relationship.getId() ) )
                {
                    continue;
                }

                final Node other = relationship.getOtherNode( node );
                if ( nodes.containsKey( other.getId() ) || newNodes.contains( other ) )
                {
                    continue;
                }
                newNodes.add( other );
            }
        }
        for ( Node node : newNodes )
        {
            add( node );
        }
    }

    private void addToGraph( Object value )
    {
        if ( value instanceof Node )
        {
            add( (Node) value );
        }
        if ( value instanceof Relationship )
        {
            add( (Relationship) value );
        }
        if ( value instanceof Iterable )
        {
            for ( Object inner : (Iterable) value )
            {
                addToGraph( inner );
            }
        }
    }

    @Override
    public Iterable<Node> getNodes()
    {
        return nodes.values();
    }

    @Override
    public Iterable<Relationship> getRelationships()
    {
        return relationships.values();
    }

    public Collection<Label> getLabels()
    {
        return labels;
    }

    void addRel( Long id, Relationship rel )
    {
        relationships.put( id, rel );
    }

    @Override
    public boolean contains( Relationship relationship )
    {
        return relationships.containsKey( relationship.getId() );
    }

    public Iterable<IndexDefinition> getIndexes()
    {
        return indexes;
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints()
    {
        return constraints;
    }

}
