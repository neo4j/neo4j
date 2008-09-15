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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PropertyIndexRecord extends AbstractRecord
{
    private int propCount = 0;
    private int keyBlockId = Record.NO_NEXT_BLOCK.intValue();
    private List<DynamicRecord> keyRecords = new ArrayList<DynamicRecord>();
    private boolean isLight = false;

    public PropertyIndexRecord( int id )
    {
        super( id );
    }

    void setIsLight( boolean status )
    {
        isLight = status;
    }

    public boolean isLight()
    {
        return isLight;
    }

    public int getPropertyCount()
    {
        return propCount;
    }

    public void setPropertyCount( int count )
    {
        this.propCount = count;
    }

    public int getKeyBlockId()
    {
        return keyBlockId;
    }

    public void setKeyBlockId( int blockId )
    {
        this.keyBlockId = blockId;
    }

    public Collection<DynamicRecord> getKeyRecords()
    {
        return keyRecords;
    }

    public void addKeyRecord( DynamicRecord record )
    {
        keyRecords.add( record );
    }

    public String toString()
    {
        StringBuffer buf = new StringBuffer();
        buf.append( "PropertyIndex[" ).append( getId() ).append( "," ).append(
            inUse() ).append( "," ).append( propCount ).append( "," ).append(
            keyBlockId ).append( "]" );
        return buf.toString();
    }
}
