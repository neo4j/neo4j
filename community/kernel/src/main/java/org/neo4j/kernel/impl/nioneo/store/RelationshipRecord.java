/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

public class RelationshipRecord extends PrimitiveRecord
{
    private long firstNode;
    private long secondNode;
    private int type;
    private long firstPrevRel = 1;
    private long firstNextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
    private long secondPrevRel = 1;
    private long secondNextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
    private boolean firstInFirstChain = true;
    private boolean firstInSecondChain = true;

    public RelationshipRecord( long id, long firstNode, long secondNode, int type )
    {
        this( id );
        this.firstNode = firstNode;
        this.secondNode = secondNode;
        this.type = type;
    }

    public RelationshipRecord( long id )
    {
        super( id, Record.NO_NEXT_PROPERTY.intValue() );
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
        return new StringBuilder( "Relationship[" )
                .append( getId() ).append( ",used=" ).append( inUse() )
                .append( ",source=" ).append( firstNode )
                .append( ",target=" ).append( secondNode )
                .append( ",type=" ).append( type )
                .append( firstInFirstChain ? ",sCount=" : ",sPrev=" ).append( firstPrevRel )
                .append( ",sNext=" ).append( firstNextRel )
                .append( firstInSecondChain ? ",tCount=" : ",tPrev=" ).append( secondPrevRel )
                .append( ",tNext=" ).append( secondNextRel )
                .append( ",prop=" ).append( getNextProp() )
                .append( firstInFirstChain ? ", sFirst" : ",!sFirst" )
                .append( firstInSecondChain ? ", tFirst" : ",!tFirst" )
                .append( "]" ).toString();
    }

    @Override
    public void setIdTo( PropertyRecord property )
    {
        property.setRelId( getId() );
    }
}