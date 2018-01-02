/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class TokenRecord extends AbstractRecord
{
    private int nameId = Record.NO_NEXT_BLOCK.intValue();
    private final List<DynamicRecord> nameRecords = new ArrayList<DynamicRecord>();
    private boolean isLight;

    TokenRecord( int id )
    {
        super( id );
    }

    public void setIsLight( boolean status )
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

    public void addNameRecords( Iterable<DynamicRecord> records )
    {
        for ( DynamicRecord record : records )
        {
            addNameRecord( record );
        }
    }

    @Override
    public String toString()
    {
        StringBuilder buf = new StringBuilder( simpleName() + "[" );
        buf.append( getId() ).append( "," ).append( inUse() ? "in" : "no" ).append( " use" );
        buf.append( ",nameId=" ).append( nameId );
        additionalToString( buf );
        if ( !isLight )
        {
            for ( DynamicRecord dyn : nameRecords )
            {
                buf.append( ',' ).append( dyn );
            }
        }
        return buf.append( ']' ).toString();
    }

    protected abstract String simpleName();

    protected void additionalToString( StringBuilder buf )
    {
        // default: nothing additional
    }
}
