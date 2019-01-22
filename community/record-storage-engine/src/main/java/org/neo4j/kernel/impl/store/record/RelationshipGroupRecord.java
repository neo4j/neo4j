/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.store.record;

import java.util.Objects;

import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

public class RelationshipGroupRecord extends AbstractBaseRecord
{
    private int type;
    private long next;
    private long firstOut;
    private long firstIn;
    private long firstLoop;
    private long owningNode;

    // Not stored, just kept in memory temporarily when loading the group chain
    private long prev;

    @Deprecated
    public RelationshipGroupRecord( long id, int type )
    {
        super( id );
        this.type = type;
    }

    @Deprecated
    public RelationshipGroupRecord( long id, int type, long firstOut, long firstIn, long firstLoop, long owningNode,
            boolean inUse )
    {
        this( id, type, firstOut, firstIn, firstLoop, owningNode, NULL_REFERENCE.intValue(), inUse );
    }

    @Deprecated
    public RelationshipGroupRecord( long id, int type, long firstOut, long firstIn, long firstLoop, long owningNode,
            long next, boolean inUse )
    {
        super( id );
        setInUse( inUse );
        this.type = type;
        this.firstOut = firstOut;
        this.firstIn = firstIn;
        this.firstLoop = firstLoop;
        this.owningNode = owningNode;
        this.next = next;
    }

    public RelationshipGroupRecord( long id )
    {
        super( id );
    }

    public RelationshipGroupRecord initialize( boolean inUse, int type,
            long firstOut, long firstIn, long firstLoop, long owningNode, long next )
    {
        super.initialize( inUse );
        this.type = type;
        this.firstOut = firstOut;
        this.firstIn = firstIn;
        this.firstLoop = firstLoop;
        this.owningNode = owningNode;
        this.next = next;
        this.prev = NULL_REFERENCE.intValue();
        return this;
    }

    @Override
    public void clear()
    {
        initialize( false, NULL_REFERENCE.intValue(), NULL_REFERENCE.intValue(), NULL_REFERENCE.intValue(),
                NULL_REFERENCE.intValue(), NULL_REFERENCE.intValue(), NULL_REFERENCE.intValue() );
        prev = NULL_REFERENCE.intValue();
    }

    public int getType()
    {
        return type;
    }

    public void setType( int type )
    {
        this.type = type;
    }

    public long getFirstOut()
    {
        return firstOut;
    }

    public void setFirstOut( long firstOut )
    {
        this.firstOut = firstOut;
    }

    public long getFirstIn()
    {
        return firstIn;
    }

    public void setFirstIn( long firstIn )
    {
        this.firstIn = firstIn;
    }

    public long getFirstLoop()
    {
        return firstLoop;
    }

    public void setFirstLoop( long firstLoop )
    {
        this.firstLoop = firstLoop;
    }

    public long getNext()
    {
        return next;
    }

    public void setNext( long next )
    {
        this.next = next;
    }

    /**
     * The previous pointer, i.e. previous group in this chain of groups isn't
     * persisted in the store, but only set during reading of the group
     * chain.
     * @param prev the id of the previous group in this chain.
     */
    public void setPrev( long prev )
    {
        this.prev = prev;
    }

    /**
     * The previous pointer, i.e. previous group in this chain of groups isn't
     * persisted in the store, but only set during reading of the group
     * chain.
     * @return the id of the previous group in this chain.
     */
    public long getPrev()
    {
        return prev;
    }

    public long getOwningNode()
    {
        return owningNode;
    }

    public void setOwningNode( long owningNode )
    {
        this.owningNode = owningNode;
    }

    @Override
    public String toString()
    {
        return "RelationshipGroup[" + getId() +
               ",type=" + type +
               ",out=" + firstOut +
               ",in=" + firstIn +
               ",loop=" + firstLoop +
               ",prev=" + prev +
               ",next=" + next +
               ",used=" + inUse() +
               ",owner=" + getOwningNode() +
               ",secondaryUnitId=" + getSecondaryUnitId() + "]";
    }

    @Override
    public RelationshipGroupRecord clone()
    {
        RelationshipGroupRecord clone = new RelationshipGroupRecord( getId() ).initialize( inUse(), type, firstOut,
                firstIn, firstLoop, owningNode, next );
        clone.setSecondaryUnitId( getSecondaryUnitId() );
        return clone;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        if ( !super.equals( o ) )
        {
            return false;
        }
        RelationshipGroupRecord that = (RelationshipGroupRecord) o;
        return type == that.type && next == that.next && firstOut == that.firstOut && firstIn == that.firstIn &&
                firstLoop == that.firstLoop && owningNode == that.owningNode;
        // don't compare prev since it's not persisted
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(), type, next, firstOut, firstIn, firstLoop, owningNode, prev );
    }
}
