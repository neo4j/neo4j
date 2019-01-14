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

import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;

public class RelationshipRecord extends PrimitiveRecord
{
    private long firstNode;
    private long secondNode;
    private int type;
    private long firstPrevRel;
    private long firstNextRel;
    private long secondPrevRel;
    private long secondNextRel;
    private boolean firstInFirstChain;
    private boolean firstInSecondChain;

    @Deprecated
    public RelationshipRecord( long id, long firstNode, long secondNode, int type )
    {
        this( id );
        this.firstNode = firstNode;
        this.secondNode = secondNode;
        this.type = type;
    }

    @Deprecated
    public RelationshipRecord( long id, boolean inUse, long firstNode, long secondNode, int type,
                               long firstPrevRel, long firstNextRel, long secondPrevRel, long secondNextRel,
                               boolean firstInFirstChain, boolean firstInSecondChain )
    {
        this( id, firstNode, secondNode, type );
        setInUse( inUse );
        this.firstPrevRel = firstPrevRel;
        this.firstNextRel = firstNextRel;
        this.secondPrevRel = secondPrevRel;
        this.secondNextRel = secondNextRel;
        this.firstInFirstChain = firstInFirstChain;
        this.firstInSecondChain = firstInSecondChain;

    }

    public RelationshipRecord( long id )
    {
        super( id );
    }

    public RelationshipRecord initialize( boolean inUse, long nextProp, long firstNode, long secondNode,
            int type, long firstPrevRel, long firstNextRel, long secondPrevRel, long secondNextRel,
            boolean firstInFirstChain, boolean firstInSecondChain )
    {
        super.initialize( inUse, nextProp );
        this.firstNode = firstNode;
        this.secondNode = secondNode;
        this.type = type;
        this.firstPrevRel = firstPrevRel;
        this.firstNextRel = firstNextRel;
        this.secondPrevRel = secondPrevRel;
        this.secondNextRel = secondNextRel;
        this.firstInFirstChain = firstInFirstChain;
        this.firstInSecondChain = firstInSecondChain;
        return this;
    }

    @Override
    public void clear()
    {
        initialize( false, NO_NEXT_PROPERTY.intValue(), -1, -1, -1,
                1, NO_NEXT_RELATIONSHIP.intValue(),
                1, NO_NEXT_RELATIONSHIP.intValue(), true, true );
    }

    public void setLinks( long firstNode, long secondNode, int type )
    {
        this.firstNode = firstNode;
        this.secondNode = secondNode;
        this.type = type;
    }

    public long getFirstNode()
    {
        return firstNode;
    }

    public void setFirstNode( long firstNode )
    {
        this.firstNode = firstNode;
    }

    public long getSecondNode()
    {
        return secondNode;
    }

    public void setSecondNode( long secondNode )
    {
        this.secondNode = secondNode;
    }

    public int getType()
    {
        return type;
    }

    public void setType( int type )
    {
        this.type = type;
    }

    public long getFirstPrevRel()
    {
        return firstPrevRel;
    }

    public void setFirstPrevRel( long firstPrevRel )
    {
        this.firstPrevRel = firstPrevRel;
    }

    public long getFirstNextRel()
    {
        return firstNextRel;
    }

    public void setFirstNextRel( long firstNextRel )
    {
        this.firstNextRel = firstNextRel;
    }

    public long getSecondPrevRel()
    {
        return secondPrevRel;
    }

    public void setSecondPrevRel( long secondPrevRel )
    {
        this.secondPrevRel = secondPrevRel;
    }

    public long getSecondNextRel()
    {
        return secondNextRel;
    }

    public void setSecondNextRel( long secondNextRel )
    {
        this.secondNextRel = secondNextRel;
    }

    public boolean isFirstInFirstChain()
    {
        return firstInFirstChain;
    }

    public void setFirstInFirstChain( boolean firstInFirstChain )
    {
        this.firstInFirstChain = firstInFirstChain;
    }

    public boolean isFirstInSecondChain()
    {
        return firstInSecondChain;
    }

    public void setFirstInSecondChain( boolean firstInSecondChain )
    {
        this.firstInSecondChain = firstInSecondChain;
    }

    @Override
    public String toString()
    {
        return "Relationship[" + getId() +
               ",used=" + inUse() +
               ",source=" + firstNode +
               ",target=" + secondNode +
               ",type=" + type +
               (firstInFirstChain ? ",sCount=" : ",sPrev=") + firstPrevRel +
               ",sNext=" + firstNextRel +
               (firstInSecondChain ? ",tCount=" : ",tPrev=") + secondPrevRel +
               ",tNext=" + secondNextRel +
               ",prop=" + getNextProp() +
               ",secondaryUnitId=" + getSecondaryUnitId() +
               (firstInFirstChain ? ", sFirst" : ",!sFirst") +
               (firstInSecondChain ? ", tFirst" : ",!tFirst") + "]";
    }

    @Override
    public RelationshipRecord clone()
    {
        RelationshipRecord record = new RelationshipRecord( getId() ).initialize( inUse(), nextProp, firstNode,
                secondNode, type, firstPrevRel, firstNextRel, secondPrevRel, secondNextRel, firstInFirstChain,
                firstInSecondChain );
        record.setSecondaryUnitId( getSecondaryUnitId() );
        return record;
    }

    @Override
    public void setIdTo( PropertyRecord property )
    {
        property.setRelId( getId() );
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
        RelationshipRecord that = (RelationshipRecord) o;
        return firstNode == that.firstNode && secondNode == that.secondNode && type == that.type &&
                firstPrevRel == that.firstPrevRel && firstNextRel == that.firstNextRel &&
                secondPrevRel == that.secondPrevRel && secondNextRel == that.secondNextRel &&
                firstInFirstChain == that.firstInFirstChain && firstInSecondChain == that.firstInSecondChain;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(), firstNode, secondNode, type, firstPrevRel, firstNextRel, secondPrevRel,
                secondNextRel, firstInFirstChain, firstInSecondChain );
    }
}
