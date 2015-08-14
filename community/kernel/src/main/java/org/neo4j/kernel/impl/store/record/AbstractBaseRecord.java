/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.helpers.CloneableInPublic;

public abstract class AbstractBaseRecord implements CloneableInPublic
{
    private boolean inUse = false;
    private boolean created = false;

    /**
     * This flag is used in consistency checker. Consistency checking code revolves around records and so
     * even in scenarios where records are built from other sources, f.ex half-and-purpose-built from cache,
     * this flag is used to signal that the real record needs to be read in order to be used as a general
     * purpose record. For now this is the best place to put this to not add burden and massive refactorings
     * to that consistency check code. If this becomes a problem memory-wise then these three booleans in here
     * could be made into one byte holding 8 flags.
     */
    private boolean realRecord = true;

    public abstract long getLongId();

    public final boolean inUse()
    {
        return inUse;
    }

    public void setInUse( boolean inUse )
    {
        this.inUse = inUse;
    }

    public final void setCreated()
    {
        this.created = true;
    }

    public final boolean isCreated()
    {
        return created;
    }

    public boolean isReal()
    {
        return realRecord;
    }

    public void setReal( boolean real )
    {
        realRecord = real;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        long id = getLongId();
        result = prime * result + (int) (id ^ (id >>> 32));
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
            return true;
        if ( obj == null )
            return false;
        if ( getClass() != obj.getClass() )
            return false;
        AbstractBaseRecord other = (AbstractBaseRecord) obj;
        if ( getLongId() != other.getLongId() )
            return false;
        return true;
    }

    @Override
    public AbstractBaseRecord clone()
    {
        throw new UnsupportedOperationException();
    }
}
