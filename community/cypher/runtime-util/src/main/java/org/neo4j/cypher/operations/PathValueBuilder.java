/*
 * Copyright (c) "Neo4j"
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

import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;

import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.util.CalledFromGeneratedCode;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.virtual.VirtualValues.pathReference;

/**
 * Builder for building paths from generated code, used when the length of the path is not known at compile time.
 * <p>
 * NOTE: this class is designed to be easy-to-use from generated code rather than from code typed by more or less
 * anthropic beings, so refactor with some care.
 */
@SuppressWarnings( {"unused", "WeakerAccess"} )
public class PathValueBuilder
{
    private final MutableLongList nodes = LongLists.mutable.empty();
    private final MutableLongList rels = LongLists.mutable.empty();
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
                           : pathReference( nodes.toArray(), rels.toArray() );
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
            addNode( (VirtualNodeValue) value );
        }
    }

    @CalledFromGeneratedCode
    public void addRelationship( AnyValue value )
    {
        if ( notNoValue( value ) )
        {
            addRelationship( (VirtualRelationshipValue) value );
        }
    }

    @CalledFromGeneratedCode
    public void addRelationship( VirtualRelationshipValue value )
    {
        rels.add( value.id() );
    }

    /**
     * Adds node to the path
     *
     * @param nodeValue the node to add
     */
    @CalledFromGeneratedCode
    public void addNode( VirtualNodeValue nodeValue )
    {
        nodes.add( nodeValue.id() );
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
            addIncoming( (VirtualRelationshipValue) value );
        }
    }

    /**
     * Adds incoming relationship to the path
     *
     * @param relationship the incoming relationship to add
     */
    @CalledFromGeneratedCode
    public void addIncoming( VirtualRelationshipValue relationship )
    {
        singleRelationship( relationship );
        nodes.add( cursor.sourceNodeReference() );
        rels.add( relationship.id() );
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
            addOutgoing( (VirtualRelationshipValue) value );
        }
    }

    /**
     * Adds outgoing relationship to the path
     *
     * @param relationship the outgoing relationship to add
     */
    @CalledFromGeneratedCode
    public void addOutgoing( VirtualRelationshipValue relationship )
    {
        singleRelationship( relationship );
        nodes.add( cursor.targetNodeReference() );
        rels.add( relationship.id() );
    }

    private void add( long relationship, long nextNode )
    {
        rels.add( relationship );
        nodes.add( nextNode );
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
            addUndirected( (VirtualRelationshipValue) value );
        }
    }

    /**
     * Adds undirected relationship to the path
     *
     * @param relationship the undirected relationship to add
     */
    @CalledFromGeneratedCode
    public void addUndirected( VirtualRelationshipValue relationship )
    {
        singleRelationship( relationship );
        long previous = nodes.get( nodes.size() - 1 );
        if ( previous == cursor.sourceNodeReference() )
        {
            add( relationship.id(), cursor.targetNodeReference() );
        }
        else if ( previous == cursor.targetNodeReference() )
        {
            add( relationship.id(), cursor.sourceNodeReference() );
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
            addMultipleIncoming( (ListValue) value, (VirtualNodeValue) target );
        }
    }

    /**
     * Adds multiple incoming relationships to the path
     *
     * @param relationships the incoming relationships to add
     * @param target the final target node of the path
     */
    @CalledFromGeneratedCode
    public void addMultipleIncoming( ListValue relationships, VirtualNodeValue target )
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
                VirtualRelationshipValue relationship = (VirtualRelationshipValue) value;
                singleRelationship( relationship );
                nodes.add( cursor.sourceNodeReference() );
                rels.add( relationship.id() );
            }
        }
        AnyValue last = relationships.value( i );
        if ( notNoValue( last ) )
        {
            rels.add( ((VirtualRelationshipValue) last).id() );
            nodes.add( target.id() );
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
                VirtualRelationshipValue relationship = (VirtualRelationshipValue) value;
                singleRelationship( relationship );
                nodes.add( cursor.sourceNodeReference() );
                rels.add( relationship.id() );
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
            addMultipleOutgoing( (ListValue) value, (VirtualNodeValue) target );
        }
    }

    /**
     * Adds multiple outgoing relationships to the path
     *
     * @param relationships the outgoing relationships to add
     * @param target the final target node of the path
     */
    @CalledFromGeneratedCode
    public void addMultipleOutgoing( ListValue relationships, VirtualNodeValue target )
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
                VirtualRelationshipValue relationship = (VirtualRelationshipValue) value;
                singleRelationship( relationship );
                nodes.add( cursor.targetNodeReference() );
                rels.add( relationship.id() );
            }
        }
        AnyValue last = relationships.value( i );
        if ( notNoValue( last ) )
        {
            rels.add( ((VirtualRelationshipValue) last).id() );
            nodes.add( target.id() );
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

                VirtualRelationshipValue relationship = (VirtualRelationshipValue) value;
                singleRelationship( relationship );
                nodes.add( cursor.targetNodeReference() );
                rels.add( relationship.id() );
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
            addMultipleUndirected( (ListValue) value, (VirtualNodeValue) target );
        }
    }

    /**
     * Adds multiple undirected relationships to the path
     *
     * @param relationships the undirected relationships to add
     * @param target the final target node of the path
     */
    @CalledFromGeneratedCode
    public void addMultipleUndirected( ListValue relationships, VirtualNodeValue target )
    {
        if ( relationships.isEmpty() )
        {
            //nothing to add
            return;
        }
        long previous = nodes.get( nodes.size() - 1 );
        VirtualRelationshipValue first = (VirtualRelationshipValue) relationships.head();
        singleRelationship( first );
        boolean correctDirection = cursor.sourceNodeReference() == previous || cursor.targetNodeReference() == previous;

        int i;
        if ( correctDirection )
        {
            for ( i = 0; i < relationships.size() - 1; i++ )
            {
                AnyValue value = relationships.value( i );
                if ( notNoValue( value ) )
                {
                    addUndirected( (VirtualRelationshipValue) value );
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
                    addUndirected( (VirtualRelationshipValue) relationships.value( i ) );
                }
            }
        }
        AnyValue last = relationships.value( i );
        if ( notNoValue( last ) )
        {
            rels.add( ((VirtualRelationshipValue) last).id() );
            nodes.add( target.id() );
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
        long previous = nodes.get( nodes.size() - 1 );
        VirtualRelationshipValue first = (VirtualRelationshipValue) relationships.head();

        singleRelationship( first );
        boolean correctDirection = cursor.sourceNodeReference() == previous || previous == cursor.targetNodeReference();

        if ( correctDirection )
        {
            for ( AnyValue value : relationships )
            {
                if ( notNoValue( value ) )
                {
                    addUndirected( (VirtualRelationshipValue) value );
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
                    addUndirected( (VirtualRelationshipValue) rel );
                }
            }
        }
    }

    //This ignores that a relationship might have been deleted here, this is weird but it is backwards compatible
    private void singleRelationship( VirtualRelationshipValue relationship )
    {
        dbAccess.singleRelationship( relationship.id(), cursor );
        cursor.next();
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
