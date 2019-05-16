/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

/**
 * {@link AbstractBaseRecord records} are intended to be reusable. Created with a zero-arg constructor
 * and initialized with the public {@code initialize} method exposed by the specific record implementations,
 * or {@link #clear() cleared} if reading a record that isn't in use.
 *
 * Regarding secondary unit there are three state fields involving secondary unit. This is an explanation of what they mean and how they relate to each other:
 * <ul>
 *     <li>{@link #requiresSecondaryUnit()} set when a record is prepared, at a time where actual ids cannot be allocated, for one or more reasons.
 *     This state is accessed at some point later and if this record doesn't have a secondary unit it assigned already, such an id will be allocated
 *     and assigned {@link #setSecondaryUnitId(long)} and the {@link #setCreatedSecondaryUnit()} flag raised because it was set right now.</li>
 *     <li>{@link #getSecondaryUnitId()} set when a large record requires a secondary unit. If such a large record is created right now
 *     (and not loaded from store) the {@link #setCreatedSecondaryUnit()} should also be set to reflect this fact.</li>
 *     <li>{@link #isSecondaryUnitCreated()} whether or not the secondary unit in this record (assuming it has one) was created now
 *     and wasn't there when it was loaded from the store</li>
 * </ul>
 */
public abstract class AbstractBaseRecord implements Cloneable
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
    private boolean createdSecondaryUnit;
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
        createdSecondaryUnit = false;
        useFixedReferences = false;
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
     * Sets a secondary record unit ID for this record on loading the record. Setting this id is separate from setting {@link #requiresSecondaryUnit()}
     * since this secondary unit id may be used to just free that id at the time of updating in the store if a record goes from two to one unit.
     */
    public void setSecondaryUnitIdOnLoad( long id )
    {
        this.secondaryUnitId = id;
    }

    /**
     * Sets a secondary record unit ID for this record on creating the secondary unit. This method also sets the {@link #setCreatedSecondaryUnit()}.
     * Setting this id is separate from setting {@link #requiresSecondaryUnit()} since this secondary unit id may be used to just free that id
     * at the time of updating in the store if a record goes from two to one unit.
     */
    public void setSecondaryUnitIdOnCreate( long id )
    {
        this.secondaryUnitId = id;
        this.createdSecondaryUnit = true;
    }

    public boolean hasSecondaryUnitId()
    {
        return secondaryUnitId != NO_ID;
    }

    /**
     * @return secondary record unit ID set by {@link #setSecondaryUnitIdOnLoad(long)} or {@link #setSecondaryUnitIdOnCreate(long)}.
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

    public boolean isSecondaryUnitCreated()
    {
        return createdSecondaryUnit;
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
        return Objects.hash( id, inUse );
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
        // Don't compare 'created' flag because it isn't properly set on reading a record from the store
        return id == other.id && inUse == other.inUse;
    }

    @Override
    public AbstractBaseRecord clone() throws CloneNotSupportedException
    {
        return (AbstractBaseRecord) super.clone();
    }
}
