/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.nioneo.store;

public class RelationshipRecord extends AbstractRecord
{
    private final int firstNode;
    private final int secondNode;
    private final int type;
    private int firstPrevRel = Record.NO_PREV_RELATIONSHIP.intValue();
    private int firstNextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
    private int secondPrevRel = Record.NO_PREV_RELATIONSHIP.intValue();
    private int secondNextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
    private int nextProp = Record.NO_NEXT_PROPERTY.intValue();

    public RelationshipRecord( int id, int firstNode, int secondNode, int type )
    {
        super( id );
        this.firstNode = firstNode;
        this.secondNode = secondNode;
        this.type = type;
    }

    public int getFirstNode()
    {
        return firstNode;
    }

    public int getSecondNode()
    {
        return secondNode;
    }

    public int getType()
    {
        return type;
    }

    public int getFirstPrevRel()
    {
        return firstPrevRel;
    }

    public void setFirstPrevRel( int firstPrevRel )
    {
        this.firstPrevRel = firstPrevRel;
    }

    public int getFirstNextRel()
    {
        return firstNextRel;
    }

    public void setFirstNextRel( int firstNextRel )
    {
        this.firstNextRel = firstNextRel;
    }

    public int getSecondPrevRel()
    {
        return secondPrevRel;
    }

    public void setSecondPrevRel( int secondPrevRel )
    {
        this.secondPrevRel = secondPrevRel;
    }

    public int getSecondNextRel()
    {
        return secondNextRel;
    }

    public void setSecondNextRel( int secondNextRel )
    {
        this.secondNextRel = secondNextRel;
    }

    public int getNextProp()
    {
        return nextProp;
    }

    public void setNextProp( int nextProp )
    {
        this.nextProp = nextProp;
    }

    @Override
    public String toString()
    {
        StringBuffer buf = new StringBuffer();
        buf.append( "RelationshipRecord[" ).append( getId() ).append( "," )
            .append( inUse() ).append( "," ).append( firstNode ).append( "," )
            .append( secondNode ).append( "," ).append( type ).append( "," )
            .append( firstPrevRel ).append( "," ).append( firstNextRel )
            .append( "," ).append( secondPrevRel ).append( "," ).append(
                secondNextRel ).append( "," ).append( nextProp ).append( "]" );
        return buf.toString();
    }
}