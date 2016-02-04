/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.function.Predicate;

import org.neo4j.helpers.CloneableInPublic;

/**
 * {@link AbstractBaseRecord records} are intended to be reusable. Created with a zero-arg constructor
 * and initialized with the public {@code initialize} method exposed by the specific record implementations,
 * or {@link #clear() cleared} if reading a record that isn't in use.
 */
public abstract class AbstractBaseRecord implements CloneableInPublic
{
    private long id;
    private boolean inUse;
    private boolean created;

    protected AbstractBaseRecord( long id )
    {
        this.id = id;
        clear();
    }

    protected AbstractBaseRecord initialize( boolean inUse )
    {
        this.inUse = inUse;
        this.created = false;
        return this;
    }

    /**
     * Clears this record to its initial state. Initializing this record with an {@code initialize-method}
     * doesn't require clear the record first, either initialize or clear suffices.
     * Subclasses, most specific subclasses only, implements this method by calling initialize with
     * zero-like arguments.
     */
    public void clear()
    {
        inUse = false;
        created = false;
    }

    public long getId()
    {
        return id;
    }

    public int getIntId()
    {
        return Math.toIntExact( id );
    }

    public void setId( long id )
    {
        this.id = id;
    }

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

    @Override
    public int hashCode()
    {
        return (int) (( id >>> 32 ) ^ id );
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
        if ( id != other.id )
            return false;
        return true;
    }

    @Override
    public AbstractBaseRecord clone()
    {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings( "rawtypes" )
    private static final Predicate IN_USE_FILTER = new Predicate<AbstractBaseRecord>()
    {
        @Override
        public boolean test( AbstractBaseRecord item )
        {
            return item.inUse();
        }
    };

    @SuppressWarnings( "rawtypes" )
    private static final Predicate NOT_IN_USE_FILTER = new Predicate<AbstractBaseRecord>()
    {
        @Override
        public boolean test( AbstractBaseRecord item )
        {
            return !item.inUse();
        }
    };

    /**
     * @return {@link Predicate filter} which only records that are {@link #inUse() in use} passes.
     */
    @SuppressWarnings( "unchecked" )
    public static <RECORD extends AbstractBaseRecord> Predicate<RECORD> inUseFilter()
    {
        return IN_USE_FILTER;
    }

    /**
     * @return {@link Predicate filter} which only records that are {@link #inUse() NOT in use} passes.
     */
    @SuppressWarnings( "unchecked" )
    public static <RECORD extends AbstractBaseRecord> Predicate<RECORD> notInUseFilter()
    {
        return NOT_IN_USE_FILTER;
    }
}
