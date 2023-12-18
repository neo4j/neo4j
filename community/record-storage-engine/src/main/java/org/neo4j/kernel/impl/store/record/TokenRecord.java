/*
 * Copyright (c) "Neo4j"
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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.neo4j.storageengine.api.Mask;

public abstract class TokenRecord extends AbstractBaseRecord
{
    private int nameId;
    private List<DynamicRecord> nameRecords;
    /**
     * This is {@code true} if this token is internal to the database, and should not show up on the product surface.
     */
    private boolean internal;

    public TokenRecord( long id )
    {
        super( id );
    }

    public TokenRecord( TokenRecord other )
    {
        super( other );
        this.nameId = other.nameId;
        this.nameRecords = new ArrayList<>( other.nameRecords.size() );
        for ( DynamicRecord record : other.nameRecords )
        {
            this.nameRecords.add( new DynamicRecord( record ) );
        }
        this.internal = other.internal;
    }

    public TokenRecord initialize( boolean inUse, int nameId )
    {
        super.initialize( inUse );
        this.nameId = nameId;
        this.nameRecords = new ArrayList<>( 1 );
        return this;
    }

    @Override
    public void clear()
    {
        initialize( false, Record.NO_NEXT_BLOCK.intValue() );
    }

    public boolean isLight()
    {
        return nameRecords == null || nameRecords.isEmpty();
    }

    public int getNameId()
    {
        return nameId;
    }

    public void setNameId( int blockId )
    {
        this.nameId = blockId;
    }

    public List<DynamicRecord> getNameRecords()
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

    public boolean isInternal()
    {
        return internal;
    }

    public void setInternal( boolean internal )
    {
        this.internal = internal;
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
        TokenRecord that = (TokenRecord) o;
        return getNameId() == that.getNameId() && isInternal() == that.isInternal() && Objects.equals( getNameRecords(), that.getNameRecords() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(), getNameId(), getNameRecords(), isInternal() );
    }

    @Override
    public String toString( Mask mask )
    {
        StringBuilder buf = new StringBuilder().append( simpleName() ).append( '[' );
        buf.append( getId() ).append( ',' ).append( inUse() ? "in" : "no" ).append( " use" );
        buf.append( ",nameId=" ).append( nameId );
        buf.append( ",internal=" ).append( internal );
        additionalToString( buf );
        if ( !isLight() )
        {
            for ( DynamicRecord dyn : nameRecords )
            {
                buf.append( ',' ).append( dyn.toString( mask ) );
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
