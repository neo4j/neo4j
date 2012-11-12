/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AbstractNameRecord extends AbstractRecord
{
    private int nameId = Record.NO_NEXT_BLOCK.intValue();
    private List<DynamicRecord> nameRecords = new ArrayList<DynamicRecord>();
    private boolean isLight;
    
    AbstractNameRecord( int id )
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
    
    public int getNameId()
    {
        return nameId;
    }

    public void setNameId( int blockId )
    {
        this.nameId = blockId;
    }

    public Collection<DynamicRecord> getNameRecords()
    {
        return nameRecords;
    }

    public void addNameRecord( DynamicRecord record )
    {
        nameRecords.add( record );
    }

    public String toString()
    {
        StringBuilder buf = new StringBuilder( getClass().getSimpleName() + "[" );
        buf.append( getId() ).append( "," ).append( inUse() ).append( "," ).append( nameId );
        String more = additionalToString();
        if ( more != null ) buf.append( "," + more );
        return buf.append( "]" ).toString();
    }

    protected String additionalToString()
    {
        return null;
    }
}
