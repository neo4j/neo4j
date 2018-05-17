/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.api.schema.index;

import org.neo4j.graphdb.Label;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static org.neo4j.internal.kernel.api.schema.SchemaUtil.idTokenNameLookup;

/**
 * A {@link Label} can have zero or more index rules which will have data specified in the rules indexed.
 */
public class StoreIndexDescriptor extends IndexDescriptor implements SchemaRule
{
    private final long id;
    private final Long owningConstraintId;
    private final String name;

    // Copy-constructor used by sub-classes.
    protected StoreIndexDescriptor( StoreIndexDescriptor indexDescriptor )
    {
        super( indexDescriptor );
        this.id = indexDescriptor.id;
        this.owningConstraintId = indexDescriptor.owningConstraintId;
        this.name = indexDescriptor.name;
    }

    // General purpose constructors.
    StoreIndexDescriptor( IndexDescriptor descriptor, long id )
    {
        this( descriptor, id, null );
    }

    StoreIndexDescriptor( IndexDescriptor descriptor, long id, Long owningConstraintId )
    {
        super( descriptor );

        this.id = id;
        this.name = descriptor.userSuppliedName.map( SchemaRule::checkName ).orElse( "index_" + id );

        if ( descriptor.providerDescriptor() == null )
        {
            throw new IllegalArgumentException( "null provider descriptor prohibited" );
        }

        this.owningConstraintId = owningConstraintId;
    }

    public boolean canSupportUniqueConstraint()
    {
        return type() == IndexDescriptor.Type.UNIQUE;
    }

    /**
     * Return the owning constraints of this index.
     *
     * The owning constraint can be null during the construction of a uniqueness constraint. This construction first
     * creates the unique index, and then waits for the index to become fully populated and online before creating
     * the actual constraint. During unique index population the owning constraint will be null.
     *
     * See ConstraintIndexCreator.createUniquenessConstraintIndex().
     *
     * @return the id of the owning constraint, or null if this has not been set yet.
     * @throws IllegalStateException if this IndexRule cannot support uniqueness constraints (ei. the index is not
     *                               unique)
     */
    public Long getOwningConstraint()
    {
        if ( !canSupportUniqueConstraint() )
        {
            throw new IllegalStateException( "Can only get owner from constraint indexes." );
        }
        return owningConstraintId;
    }

    public boolean isIndexWithoutOwningConstraint()
    {
        return canSupportUniqueConstraint() && getOwningConstraint() == null;
    }

    public StoreIndexDescriptor withOwningConstraint( Long constraintId )
    {
        if ( !canSupportUniqueConstraint() )
        {
            throw new IllegalStateException( this + " is not a constraint index" );
        }
        return new StoreIndexDescriptor( this, id, constraintId );
    }

    @Override
    public String toString()
    {
        String ownerString = "";
        if ( canSupportUniqueConstraint() )
        {
            ownerString = ", owner=" + owningConstraintId;
        }

        return "IndexDescriptor[id=" + id + ", descriptor=" + this.userDescription( idTokenNameLookup ) +
               ", provider=" + this.providerDescriptor() + ownerString + "]";
    }

    @Override
    public long getId()
    {
        return id;
    }

    @Override
    public String getName()
    {
        return name;
    }

    public CapableIndexDescriptor withoutCapabilities()
    {
        return new CapableIndexDescriptor( this, IndexCapability.NO_CAPABILITY );
    }

    public CapableIndexDescriptor withCapabilities( IndexProviderMap indexProviderMap )
    {
        IndexCapability capability = indexProviderMap.lookup( providerDescriptor ).getCapability();
        return new CapableIndexDescriptor( this, capability );
    }
}
