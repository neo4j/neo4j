/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

public abstract class PrimitiveRecord extends AbstractBaseRecord
{
    protected long nextProp;

    PrimitiveRecord( long id )
    {
        super( id );
    }

    public PrimitiveRecord( PrimitiveRecord other )
    {
        super( other );
        this.nextProp = other.nextProp;
    }

    @Override
    public void clear()
    {
        super.clear();
        nextProp = Record.NO_NEXT_PROPERTY.intValue();
    }

    protected PrimitiveRecord initialize( boolean inUse, long nextProp )
    {
        super.initialize( inUse );
        this.nextProp = nextProp;
        return this;
    }

    public long getNextProp()
    {
        return nextProp;
    }

    public void setNextProp( long nextProp )
    {
        this.nextProp = nextProp;
    }

    public abstract void setIdTo( PropertyRecord property );

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(), nextProp );
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( !super.equals( obj ) )
        {
            return false;
        }
        PrimitiveRecord other = (PrimitiveRecord) obj;
        return nextProp == other.nextProp;
    }
}
