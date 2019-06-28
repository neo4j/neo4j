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

import java.util.Optional;
import java.util.OptionalLong;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexDescriptor2;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;

import static org.neo4j.common.TokenNameLookup.idTokenNameLookup;

/**
 * Describes an index which is committed to the database.
 *
 * Adds an index id, a name, and optionally an owning constraint id to the general IndexDescriptor.
 */
@Deprecated
public class StoreIndexDescriptor extends IndexDescriptor
{
    private final long id;
    private final Long owningConstraintId;

    // ** Copy-constructors used by sub-classes.
    protected StoreIndexDescriptor( StoreIndexDescriptor indexDescriptor )
    {
        this( indexDescriptor, indexDescriptor.id, indexDescriptor.owningConstraintId );
    }

    protected StoreIndexDescriptor( IndexDescriptor descriptor, long id, Long owningConstraintId )
    {
        super( descriptor, descriptor.providerDescriptor() );
        this.id = id;
        this.owningConstraintId = owningConstraintId;
    }

    // ** General purpose constructors.

    public StoreIndexDescriptor( IndexDescriptor2 descriptor )
    {
        super( descriptor.schema(), descriptor.isUnique(), Optional.of( descriptor.getName() ), descriptor.getIndexProvider() );
        this.id = descriptor.getId();
        OptionalLong owningConstraintId = descriptor.getOwningConstraintId();
        this.owningConstraintId = owningConstraintId.isPresent() ? owningConstraintId.getAsLong() : null;
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
    public String getName()
    {
        // Override the otherwise bland default to provide a bit more information now that we at least know the id
        return name.orElse( "index_" + id );
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

    @Override
    public StoreIndexDescriptor withIndexProvider( IndexProviderDescriptor indexProvider )
    {
        return new StoreIndexDescriptor( super.withIndexProvider( indexProvider ), id, owningConstraintId );
    }

    @Override
    public StoreIndexDescriptor withSchemaDescriptor( SchemaDescriptor schema )
    {
        return new StoreIndexDescriptor( super.withSchemaDescriptor( schema ), id, owningConstraintId );
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
        return toString( idTokenNameLookup );
    }

    public String toString( TokenNameLookup tokenNameLookup )
    {
        String ownerString = "";
        if ( isUnique() )
        {
            ownerString = ", owner=" + owningConstraintId;
        }

        return "IndexRule[id=" + id + ", descriptor=" + this.userDescription( tokenNameLookup ) +
                ", provider=" + this.providerDescriptor() + ownerString + "]";
    }

    public long getId()
    {
        return id;
    }

    public long indexReference()
    {
        return id;
    }
}
