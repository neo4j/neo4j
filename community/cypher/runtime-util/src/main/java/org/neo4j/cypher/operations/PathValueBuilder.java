/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.operations;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;

import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.virtual.VirtualValues.path;

/**
 * Builder for building paths from generated code, used when the length of the path is not known at compile time.
 * <p>
 * NOTE: this class is designed to be easy-to-use from generated code rather than from code typed by more or less
 * anthropic beings, so refactor with some care.
 */
@SuppressWarnings( {"unused", "WeakerAccess"} )
public class PathValueBuilder
{
    private final List<NodeValue> nodes = new ArrayList<>();
    private final List<RelationshipValue> rels = new ArrayList<>();
    private boolean seenNoValue;

    /**
     * Creates a PathValue or NO_VALUE if any NO_VALUES has been encountered.
     *
     * @return a PathValue or NO_VALUE if any NO_VALUES has been encountered
     */
    public AnyValue build()
    {
        return seenNoValue ? NO_VALUE
                           : path( nodes.toArray( new NodeValue[0] ), rels.toArray( new RelationshipValue[0] ) );
    }

    /**
     * Adds node to the path
     *
     * @param value the node to add
     */
    public void addNode( AnyValue value )
    {
        if ( notNoValue( value ) )
        {
            addNode( (NodeValue) value );
        }
    }

    /**
     * Adds node to the path
     *
     * @param nodeValue the node to add
     */
    public void addNode( NodeValue nodeValue )
    {
        nodes.add( nodeValue );
    }

    /**
     * Adds incoming relationship to the path
     *
     * @param value the incoming relationship to add
     */
    public void addIncoming( AnyValue value )
    {
        if ( notNoValue( value ) )
        {
            addIncoming( (RelationshipValue) value );
        }
    }

    /**
     * Adds incoming relationship to the path
     *
     * @param relationship the incoming relationship to add
     */
    public void addIncoming( RelationshipValue relationship )
    {
        nodes.add( relationship.startNode() );
        rels.add( relationship );
    }

    /**
     * Adds outgoing relationship to the path
     *
     * @param value the outgoing relationship to add
     */
    public void addOutgoing( AnyValue value )
    {
        if ( notNoValue( value ) )
        {
            addOutgoing( (RelationshipValue) value );
        }
    }

    /**
     * Adds outgoing relationship to the path
     *
     * @param relationship the outgoing relationship to add
     */
    public void addOutgoing( RelationshipValue relationship )
    {
        nodes.add( relationship.endNode() );
        rels.add( relationship );
    }

    /**
     * Adds undirected relationship to the path
     *
     * @param value the undirected relationship to add
     */
    public void addUndirected( AnyValue value )
    {
        if ( notNoValue( value ) )
        {
            addUndirected( (RelationshipValue) value );
        }
    }

    /**
     * Adds undirected relationship to the path
     *
     * @param relationship the undirected relationship to add
     */
    public void addUndirected( RelationshipValue relationship )
    {
        long previous = nodes.get( nodes.size() - 1 ).id();
        if ( previous == relationship.startNode().id() )
        {
            addOutgoing( relationship );
        }
        else if ( previous == relationship.endNode().id() )
        {
            addIncoming( relationship );
        }
        else
        {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Adds multiple incoming relationships to the path
     *
     * @param value the incoming relationships to add
     */
    public void addMultipleIncoming( AnyValue value )
    {
        if ( notNoValue( value ) )
        {
            addMultipleIncoming( (ListValue) value );
        }
    }

    /**
     * Adds multiple incoming relationships to the path
     *
     * @param relationships the incoming relationships to add
     */
    public void addMultipleIncoming( ListValue relationships )
    {
        for ( AnyValue rel : relationships )
        {
            addIncoming( rel );
        }
    }

    /**
     * Adds multiple outgoing relationships to the path
     *
     * @param value the outgoing relationships to add
     */
    public void addMultipleOutgoing( AnyValue value )
    {
        if ( notNoValue( value ) )
        {
            addMultipleOutgoing( (ListValue) value );
        }
    }

    /**
     * Adds multiple outgoing relationships to the path
     *
     * @param relationships the outgoing relationships to add
     */
    public void addMultipleOutgoing( ListValue relationships )
    {
        for ( AnyValue rel : relationships )
        {
            addOutgoing( rel );
        }
    }

    /**
     * Adds multiple undirected relationships to the path
     *
     * @param value the undirected relationships to add
     */
    public void addMultipleUndirected( AnyValue value )
    {
        if ( notNoValue( value ) )
        {
            addMultipleUndirected( (ListValue) value );
        }
    }

    /**
     * Adds multiple undirected relationships to the path
     *
     * @param relationships the undirected relationships to add
     */
    public void addMultipleUndirected( ListValue relationships )
    {
        if ( relationships.isEmpty() )
        {
            //nothing to add
            return;
        }
        long previous = nodes.get( nodes.size() - 1 ).id();
        RelationshipValue first = (RelationshipValue) relationships.head();
        boolean correctDirection =
                first.startNode().id() == previous ||
                first.endNode().id() == previous;

        if ( correctDirection )
        {
            for ( AnyValue rel : relationships )
            {
                addUndirected( rel );
            }
        }
        else
        {
            ListValue reversed = relationships.reverse();
            for ( AnyValue rel : reversed )
            {
                addUndirected( rel );
            }
        }
    }

    private boolean notNoValue( AnyValue value )
    {
        if ( !seenNoValue && value == NO_VALUE )
        {
            seenNoValue = true;
        }
        return !seenNoValue;
    }
}

