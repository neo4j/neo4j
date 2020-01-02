/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.util.CalledFromGeneratedCode;
import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;

import static org.neo4j.cypher.operations.CypherFunctions.endNode;
import static org.neo4j.cypher.operations.CypherFunctions.startNode;
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
    private final DbAccess dbAccess;
    private final RelationshipScanCursor cursor;
    private boolean seenNoValue;

    public PathValueBuilder( DbAccess dbAccess, RelationshipScanCursor cursor )
    {
        this.dbAccess = dbAccess;
        this.cursor = cursor;
    }

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
    @CalledFromGeneratedCode
    public void addNode( AnyValue value )
    {
        if ( notNoValue( value ) )
        {
            addNode( (NodeValue) value );
        }
    }

    @CalledFromGeneratedCode
    public void addRelationship( AnyValue value )
    {
        if ( notNoValue( value ) )
        {
            addRelationship( (RelationshipValue) value );
        }
    }

    @CalledFromGeneratedCode
    public void addRelationship( RelationshipValue value )
    {
        rels.add( value );
    }

    /**
     * Adds node to the path
     *
     * @param nodeValue the node to add
     */
    @CalledFromGeneratedCode
    public void addNode( NodeValue nodeValue )
    {
        nodes.add( nodeValue );
    }

    /**
     * Adds incoming relationship to the path
     *
     * @param value the incoming relationship to add
     */
    @CalledFromGeneratedCode
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
    @CalledFromGeneratedCode
    public void addIncoming( RelationshipValue relationship )
    {
        nodes.add( startNode( relationship, dbAccess, cursor ) );
        rels.add( relationship );
    }

    /**
     * Adds outgoing relationship to the path
     *
     * @param value the outgoing relationship to add
     */
    @CalledFromGeneratedCode
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
    @CalledFromGeneratedCode
    public void addOutgoing( RelationshipValue relationship )
    {
        nodes.add( endNode(relationship, dbAccess, cursor ));
        rels.add( relationship );
    }

    /**
     * Adds undirected relationship to the path
     *
     * @param value the undirected relationship to add
     */
    @CalledFromGeneratedCode
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
    @CalledFromGeneratedCode
    public void addUndirected( RelationshipValue relationship )
    {
        long previous = nodes.get( nodes.size() - 1 ).id();
        if ( previous == startNode( relationship, dbAccess, cursor ).id() )
        {
            addOutgoing( relationship );
        }
        else if ( previous == endNode( relationship, dbAccess, cursor ).id() )
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
     * @param target the final target node of the path
     */
    @CalledFromGeneratedCode
    public void addMultipleIncoming( AnyValue value, AnyValue target )
    {
        if ( notNoValue( value ) && notNoValue( target ) )
        {
            addMultipleIncoming( (ListValue) value, (NodeValue) target );
        }
    }

    /**
     * Adds multiple incoming relationships to the path
     *
     * @param relationships the incoming relationships to add
     * @param target the final target node of the path
     */
    @CalledFromGeneratedCode
    public void addMultipleIncoming( ListValue relationships, NodeValue target )
    {
        if ( relationships.isEmpty() )
        {
            //nothing to do here
            return;
        }
        int i;
        for ( i = 0; i < relationships.size() - 1; i++ )
        {
            AnyValue value = relationships.value( i );
            if ( notNoValue( value ) )
            {
                RelationshipValue relationship = (RelationshipValue) value;
                nodes.add( relationship.startNode() );
                rels.add( relationship );
            }
        }
        AnyValue last = relationships.value( i );
        if ( notNoValue( last ) )
        {
            rels.add( (RelationshipValue) last );
            nodes.add( target );
        }
    }

    /**
     * Adds multiple incoming relationships to the path
     *
     * @param value the incoming relationships to add
     */
    @CalledFromGeneratedCode
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
    @CalledFromGeneratedCode
    public void addMultipleIncoming( ListValue relationships )
    {
        for ( AnyValue value : relationships )
        {
            if ( notNoValue( value ) )
            {
                //we know these relationships have already loaded start and end relationship
                //so we should not use CypherFunctions::[start,end]Node to look them up
                RelationshipValue relationship = (RelationshipValue) value;
                nodes.add( relationship.startNode() );
                rels.add( relationship );
            }
        }
    }

    /**
     * Adds multiple outgoing relationships to the path
     *
     * @param value the outgoing relationships to add
     * @param target the final target node of the path
     */
    @CalledFromGeneratedCode
    public void addMultipleOutgoing( AnyValue value, AnyValue target )
    {
        if ( notNoValue( value ) && notNoValue( target ) )
        {
            addMultipleOutgoing( (ListValue) value, (NodeValue) target );
        }
    }

    /**
     * Adds multiple outgoing relationships to the path
     *
     * @param relationships the outgoing relationships to add
     * @param target the final target node of the path
     */
    @CalledFromGeneratedCode
    public void addMultipleOutgoing( ListValue relationships, NodeValue target )
    {
        if ( relationships.isEmpty() )
        {
            //nothing to do here
            return;
        }
        int i;
        for ( i = 0; i < relationships.size() - 1; i++ )
        {
            AnyValue value = relationships.value( i );
            if ( notNoValue( value ) )
            {
                //we know these relationships have already loaded start and end relationship
                //so we should not use CypherFunctions::[start,end]Node to look them up
                RelationshipValue relationship = (RelationshipValue) value;
                nodes.add( relationship.endNode() );
                rels.add( relationship );
            }
        }
        AnyValue last = relationships.value( i );
        if ( notNoValue( last ) )
        {
            rels.add( (RelationshipValue) last );
            nodes.add( target );
        }
    }

    /**
     * Adds multiple outgoing relationships to the path
     *
     * @param value the outgoing relationships to add
     */
    @CalledFromGeneratedCode
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
    @CalledFromGeneratedCode
    public void addMultipleOutgoing( ListValue relationships )
    {
        for ( AnyValue value : relationships )
        {
            if ( notNoValue( value ) )
            {
                //we know these relationships have already loaded start and end relationship
                //so we should not use CypherFunctions::[start,end]Node to look them up
                RelationshipValue relationship = (RelationshipValue) value;
                nodes.add( relationship.endNode() );
                rels.add( relationship );
            }
        }
    }

    /**
     * Adds multiple undirected relationships to the path
     *
     * @param value the undirected relationships to add
     * @param target the final target node of the path
     */
    @CalledFromGeneratedCode
    public void addMultipleUndirected( AnyValue value, AnyValue target )
    {
        if ( notNoValue( value ) && notNoValue( target ) )
        {
            addMultipleUndirected( (ListValue) value, (NodeValue) target );
        }
    }

    /**
     * Adds multiple undirected relationships to the path
     *
     * @param relationships the undirected relationships to add
     * @param target the final target node of the path
     */
    @CalledFromGeneratedCode
    public void addMultipleUndirected( ListValue relationships, NodeValue target )
    {
        if ( relationships.isEmpty() )
        {
            //nothing to add
            return;
        }
        long previous = nodes.get( nodes.size() - 1 ).id();
        RelationshipValue first = (RelationshipValue) relationships.head();
        boolean correctDirection =
                startNode( first, dbAccess, cursor ).id() == previous ||
                endNode(first, dbAccess, cursor ).id() == previous;

        int i;
        if ( correctDirection )
        {
            for ( i = 0; i < relationships.size() - 1; i++ )
            {
                AnyValue value = relationships.value( i );
                if ( notNoValue( value ) )
                {
                    //we know these relationships have already loaded start and end relationship
                    //so we should not use CypherFunctions::[start,end]Node to look them up
                    addUndirectedWhenRelationshipsAreFullyLoaded( (RelationshipValue) value );
                }
            }
        }
        else
        {
            for ( i = relationships.size() - 1; i > 0; i-- )
            {
                AnyValue value = relationships.value( i );
                if ( notNoValue( value ) )
                {
                    //we know these relationships have already loaded start and end relationship
                    //so we should not use CypherFunctions::[start,end]Node to look them up
                    addUndirectedWhenRelationshipsAreFullyLoaded( (RelationshipValue) relationships.value( i ) );
                }
            }
        }
        AnyValue last = relationships.value( i );
        if ( notNoValue( last ) )
        {
            rels.add( (RelationshipValue) last );
            nodes.add( target );
        }
    }

    /**
     * Adds multiple undirected relationships to the path
     *
     * @param value the undirected relationships to add
     */
    @CalledFromGeneratedCode
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
    @CalledFromGeneratedCode
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
                startNode( first, dbAccess, cursor ).id() == previous ||
                endNode( first, dbAccess, cursor ).id() == previous;

        if ( correctDirection )
        {
            for ( AnyValue value : relationships )
            {
                if ( notNoValue( value ) )
                {
                    addUndirectedWhenRelationshipsAreFullyLoaded( (RelationshipValue) value );
                }
            }
        }
        else
        {
            ListValue reversed = relationships.reverse();
            for ( AnyValue rel : reversed )
            {
                if ( notNoValue( rel ) )
                {
                    addUndirectedWhenRelationshipsAreFullyLoaded( (RelationshipValue) rel );
                }
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

    /*
     * If we know that relationship has loaded start and end node we can use this method instead
     */
    private void addUndirectedWhenRelationshipsAreFullyLoaded( RelationshipValue relationship )
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
}

