/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

public class RelationshipGroupRecord extends Abstract64BitRecord
{
    private final int type;
    private long next = Record.NO_NEXT_RELATIONSHIP.intValue();
    private long firstOut = Record.NO_NEXT_RELATIONSHIP.intValue();
    private long firstIn = Record.NO_NEXT_RELATIONSHIP.intValue();
    private long firstLoop = Record.NO_NEXT_RELATIONSHIP.intValue();
    private long owningNode = Record.NO_NEXT_RELATIONSHIP.intValue();

    // Not stored, just kept in memory temporarily when loading the group chain
    private long prev = Record.NO_NEXT_RELATIONSHIP.intValue();

    public RelationshipGroupRecord( long id, int type )
    {
        super( id );
        this.type = type;
    }

    public int getType()
    {
        return type;
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
        return new StringBuilder( "RelationshipGroup[" + getId() )
                .append( ",type=" + type )
                .append( ",out=" + firstOut )
                .append( ",in=" + firstIn )
                .append( ",loop=" + firstLoop )
                .append( ",prev=" + prev )
                .append( ",next=" + next )
                .append( ",used=" + inUse() )
                .append( ",owner=" + getOwningNode() )
                .append( "]" ).toString();
    }
}
