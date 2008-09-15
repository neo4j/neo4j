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

public class NodeRecord extends AbstractRecord
{
    private int nextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
    private int nextProp = Record.NO_NEXT_PROPERTY.intValue();

    public NodeRecord( int id )
    {
        super( id );
    }

    public int getNextRel()
    {
        return nextRel;
    }

    public void setNextRel( int nextRel )
    {
        this.nextRel = nextRel;
    }

    public int getNextProp()
    {
        return nextProp;
    }

    public void setNextProp( int nextProp )
    {
        this.nextProp = nextProp;
    }

    public String toString()
    {
        StringBuffer buf = new StringBuffer();
        buf.append( "NodeRecord[" ).append( getId() ).append( "," ).append(
            inUse() ).append( "," ).append( nextRel ).append( "," ).append(
            nextProp ).append( "]" );
        return buf.toString();
    }
}