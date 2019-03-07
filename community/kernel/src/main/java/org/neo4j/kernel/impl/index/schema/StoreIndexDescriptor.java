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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.storageengine.api.StorageIndexReference;

/**
 * Describes an index which is committed to the database.
 *
 * Adds an index id, a name, and optionally an owning constraint id to the general IndexDescriptor.
 */
public class StoreIndexDescriptor extends IndexDescriptor implements StorageIndexReference
{
    private final long id;
    private final Long owningConstraintId;

    // ** Copy-constructor used by sub-classes.
    protected StoreIndexDescriptor( StoreIndexDescriptor indexDescriptor )
    {
        this( indexDescriptor, indexDescriptor.id, indexDescriptor.owningConstraintId );
    }

    // ** General purpose constructors.

    /**
     * Convert a {@link StorageIndexReference} to a {@link StoreIndexDescriptor}.
     */
    public StoreIndexDescriptor( StorageIndexReference descriptor )
    {
        this( descriptor, descriptor.indexReference(), null );
    }

    /**
     * Convert a {@link StorageIndexReference} to a {@link StoreIndexDescriptor} having an owning constraint id.
     */
    public StoreIndexDescriptor( StorageIndexReference descriptor, Long owningConstraintId )
    {
        this( descriptor, descriptor.indexReference(), owningConstraintId );
    }

    /**
     * Convert a non-committed {@link org.neo4j.internal.schema.IndexDescriptor} to a {@link StoreIndexDescriptor},
     * supplying an id, which effectively makes it act like a committed descriptor.
     */
    public StoreIndexDescriptor( org.neo4j.internal.schema.IndexDescriptor descriptor, long id )
    {
        this( descriptor, id, null );
    }

    /**
     * Convert a non-committed {@link org.neo4j.internal.schema.IndexDescriptor} to a {@link StoreIndexDescriptor},
     * supplying an id and owning constraint, which effectively makes it act like a committed descriptor.
     */
    public StoreIndexDescriptor( org.neo4j.internal.schema.IndexDescriptor descriptor, long id, Long owningConstraintId )
    {
        super( descriptor );

        this.id = id;
        name.ifPresent( SchemaRule::checkName );

        if ( descriptor.providerKey() == null || descriptor.providerVersion() == null )
        {
            throw new IllegalArgumentException( "null provider descriptor prohibited" );
        }

        if ( owningConstraintId != null )
        {
            assertValidId( owningConstraintId, "owning constraint id" );
        }

        this.owningConstraintId = owningConstraintId;
    }

    @Override
    public String name()
    {
        // Override the otherwise bland default to provide a bit more information now that we at least know the id
        return name.orElse( "index_" + id );
    }

    // ** Owning constraint

    @Override
    public boolean hasOwningConstraintReference()
    {
        assertUniqueTypeIndex();
        return owningConstraintId != null;
    }

    private void assertUniqueTypeIndex()
    {
        if ( !isUnique() )
        {
            throw new IllegalStateException( "Can only get owner from constraint indexes." );
        }
    }

    /**
     * Return the owning constraints of this index.
     *
     * The owning constraint can be null during the construction of a uniqueness constraint. This construction first
     * creates the unique index, and then waits for the index to become fully populated and online before creating
     * the actual constraint. During unique index population the owning constraint will be null.
     *
     * @return the id of the owning constraint, or null if this has not been set yet.
     * @throws IllegalStateException if this IndexRule cannot support uniqueness constraints (ei. the index is not
     *                               unique)
     */
    @Override
    public long owningConstraintReference()
    {
        assertUniqueTypeIndex();
        if ( owningConstraintId == null )
        {
            throw new IllegalStateException( "No owning constraint for this descriptor" );
        }
        return owningConstraintId;
    }

    /**
     * Create a {@link StoreIndexDescriptor} with the given owning constraint id.
     *
     * @param constraintId an id >= 0, or null if no owning constraint exists.
     * @return a new StoreIndexDescriptor with modified owning constraint.
     */
    public StoreIndexDescriptor withOwningConstraint( Long constraintId )
    {
        assertUniqueTypeIndex();
        return new StoreIndexDescriptor( this, constraintId );
    }

    // ** Upgrade to capable

    /**
     * Create a {@link CapableIndexDescriptor} from this index descriptor, with no listed capabilities.
     *
     * @return a CapableIndexDescriptor.
     */
    public CapableIndexDescriptor withoutCapabilities()
    {
        return new CapableIndexDescriptor( this, IndexCapability.NO_CAPABILITY );
    }

    // ** Misc

    /**
     * WARNING: This toString is currently used in the inconsistency report, and cannot be changed due to backwards
     *          compatibility. If you are also annoyed by this, maybe now is the time to fix the inconsistency checker.
     *
     * see InconsistencyReportReader.propagate( String, long )
     */
    @Override
    public String toString()
    {
        String ownerString = "";
        if ( isUnique() )
        {
            ownerString = ", owner=" + owningConstraintId;
        }

        return "IndexRule[id=" + id + ", descriptor=" + this.userDescription( TokenNameLookup.idTokenNameLookup ) +
                ", provider=" + this.providerDescriptor() + ownerString + "]";
    }

    @Override
    public long getId()
    {
        return id;
    }

    @Override
    public long indexReference()
    {
        return id;
    }
}
