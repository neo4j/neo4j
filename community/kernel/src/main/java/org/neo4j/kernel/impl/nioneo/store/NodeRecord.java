/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

public class NodeRecord extends Abstract64BitRecord
{
    private long nextRel = Record.NO_NEXT_RELATIONSHIP.intValue();
    private long nextProp = Record.NO_NEXT_PROPERTY.intValue();

    public NodeRecord( long id )
    {
        super( id );
    }

    public long getNextRel()
    {
        return nextRel;
    }

    public void setNextRel( long nextRel )
    {
        this.nextRel = nextRel;
    }

    public long getNextProp()
    {
        return nextProp;
    }

    public void setNextProp( long nextProp )
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