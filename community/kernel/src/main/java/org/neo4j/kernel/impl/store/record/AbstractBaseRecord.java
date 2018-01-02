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

import java.util.function.Predicate;

import org.neo4j.helpers.CloneableInPublic;

/**
 * {@link AbstractBaseRecord records} are intended to be reusable. Created with a zero-arg constructor
 * and initialized with the public {@code initialize} method exposed by the specific record implementations,
 * or {@link #clear() cleared} if reading a record that isn't in use.
 */
public abstract class AbstractBaseRecord implements CloneableInPublic
{
    public static final int NO_ID = -1;
    private long id;
    // Used for the "record unit" feature where one logical record may span two physical records,
    // as to still keep low and fixed record size, but support occasionally bigger records.
    private long secondaryUnitId;
    // This flag is for when a record required a secondary unit, was changed, as a result of that change
    // no longer requires that secondary unit and gets updated. In that scenario we still want to know
    // about the secondary unit id so that we can free it when the time comes to apply the record to store.
    private boolean requiresSecondaryUnit;
    private boolean inUse;
    private boolean created;
    // Flag that indicates usage of fixed references format.
    // Fixed references format allows to avoid encoding/decoding of references in variable length format and as result
    // speed up records read/write operations.
    private boolean useFixedReferences;

    protected AbstractBaseRecord( long id )
    {
        this.id = id;
        clear();
    }

    protected AbstractBaseRecord initialize( boolean inUse )
    {
        this.inUse = inUse;
        this.created = false;
        this.secondaryUnitId = NO_ID;
        this.requiresSecondaryUnit = false;
        this.useFixedReferences = false;
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
        secondaryUnitId = NO_ID;
        requiresSecondaryUnit = false;
        this.useFixedReferences = false;
    }

    public long getId()
    {
        return id;
    }

    public int getIntId()
    {
        return Math.toIntExact( id );
    }

    public final void setId( long id )
    {
        this.id = id;
    }

    /**
     * Sets a secondary record unit ID for this record. If this is set to something other than {@link #NO_ID}
     * then {@link #requiresSecondaryUnit()} will return {@code true}.
     * Setting this id is separate from setting {@link #requiresSecondaryUnit()} since this secondary unit id
     * may be used to just free that id at the time of updating in the store if a record goes from two to one unit.
     */
    public void setSecondaryUnitId( long id )
    {
        this.secondaryUnitId = id;
    }

    public boolean hasSecondaryUnitId()
    {
        return secondaryUnitId != NO_ID;
    }

    /**
     * @return secondary record unit ID set by {@link #setSecondaryUnitId(long)}.
     */
    public long getSecondaryUnitId()
    {
        return this.secondaryUnitId;
    }

    public void setRequiresSecondaryUnit( boolean requires )
    {
        this.requiresSecondaryUnit = requires;
    }

    /**
     * @return whether or not a secondary record unit ID has been assigned.
     */
    public boolean requiresSecondaryUnit()
    {
        return requiresSecondaryUnit;
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

    public boolean isUseFixedReferences()
    {
        return useFixedReferences;
    }

    public void setUseFixedReferences( boolean useFixedReferences )
    {
        this.useFixedReferences = useFixedReferences;
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
        {
            return true;
        }
        if ( obj == null )
        {
            return false;
        }
        if ( getClass() != obj.getClass() )
        {
            return false;
        }
        AbstractBaseRecord other = (AbstractBaseRecord) obj;
        if ( id != other.id )
        {
            return false;
        }
        return true;
    }

    @Override
    public AbstractBaseRecord clone()
    {
        throw new UnsupportedOperationException();
    }

    private static final Predicate IN_USE_FILTER = (Predicate<AbstractBaseRecord>) AbstractBaseRecord::inUse;

    @SuppressWarnings( "rawtypes" )
    private static final Predicate NOT_IN_USE_FILTER = (Predicate<AbstractBaseRecord>) item -> !item.inUse();

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
