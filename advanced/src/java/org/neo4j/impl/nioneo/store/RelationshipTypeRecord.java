/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RelationshipTypeRecord extends AbstractRecord
{
    private int typeBlock = Record.NO_NEXT_BLOCK.intValue();
    private Map<Integer,DynamicRecord> typeRecords = 
        new HashMap<Integer,DynamicRecord>();

    public RelationshipTypeRecord( int id )
    {
        super( id );
    }

    public DynamicRecord getTypeRecord( int blockId )
    {
        return typeRecords.get( blockId );
    }

    public void addTypeRecord( DynamicRecord record )
    {
        typeRecords.put( record.getId(), record );
    }

    public int getTypeBlock()
    {
        return typeBlock;
    }

    public void setTypeBlock( int typeBlock )
    {
        this.typeBlock = typeBlock;
    }

    public Collection<DynamicRecord> getTypeRecords()
    {
        return typeRecords.values();
    }

    @Override
    public String toString()
    {
        StringBuffer buf = new StringBuffer();
        buf.append( "RelationshipTypeRecord[" ).append( getId() ).append( "," )
            .append( inUse() ).append( "," ).append( typeBlock );
        buf.append( ", blocks[" );
        for ( DynamicRecord record : typeRecords.values() )
        {
            buf.append( record );
        }
        buf.append( "]]" );
        return buf.toString();
    }
}