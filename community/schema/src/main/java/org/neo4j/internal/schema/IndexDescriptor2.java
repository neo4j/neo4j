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
package org.neo4j.internal.schema;

import java.util.Objects;
import java.util.OptionalLong;

import org.neo4j.common.TokenNameLookup;

public class IndexDescriptor2 implements SchemaDescriptorSupplier
{
    private final long id;
    private final String name;
    private final SchemaDescriptor schema;
    private final boolean isUnique;
    private final IndexProviderDescriptor indexProvider;
    private final Long owningConstraintId;
    private final IndexCapability capability;

    IndexDescriptor2( long id, IndexPrototype prototype )
    {
        // TODO we should throw an exception instead of generating a name for unnamed index prototypes.
        this( id, prototype.getName().orElseGet( () -> "index_" + id ), prototype.schema(), prototype.isUnique(), prototype.getIndexProvider(), null,
                IndexCapability.NO_CAPABILITY );
    }

    private IndexDescriptor2( long id, String name, SchemaDescriptor schema, boolean isUnique, IndexProviderDescriptor indexProvider, Long owningConstraintId,
            IndexCapability capability )
    {
        if ( id < 1 )
        {
            throw new IllegalArgumentException( "The id of an index must be positive, but it was attempted to assign " + id + "." );
        }
        SchemaRule.checkName( name );
        Objects.requireNonNull( schema, "The schema of an index cannot be null." );
        Objects.requireNonNull( indexProvider, "The index provider cannot be null." );
        // The 'owningConstraintId' is allowed to be null, which is the case when an index descriptor is initially created.
        Objects.requireNonNull( capability, "The index capability cannot be null." );

        this.id = id;
        this.name = name;
        this.schema = schema;
        this.isUnique = isUnique;
        this.indexProvider = indexProvider;
        this.owningConstraintId = owningConstraintId;
        this.capability = capability;
    }

    @Override
    public SchemaDescriptor schema()
    {
        return schema;
    }

    /**
     * Returns true if this index is only meant to allow one value per key.
     */
    public boolean isUnique()
    {
        return isUnique;
    }

    /**
     * @return The name of this index.
     */
    public String getName()
    {
        return name;
    }

    /**
     * @return The id of the constraint that owns this index, if such a constraint exists. Otherwise {@code empty}.
     */
    public OptionalLong getOwningConstraintId()
    {
        return owningConstraintId == null ? OptionalLong.empty() : OptionalLong.of( owningConstraintId );
    }

    /**
     * Returns a user friendly description of this index descriptor.
     *
     * @param tokenNameLookup used for looking up names for token ids.
     * @return a user friendly description of what this index indexes.
     */
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return "Index( " + id + ", '" + name + "', " + (isUnique ? "UNIQUE" : "GENERAL") + ", " + schema().userDescription( tokenNameLookup ) + " )";
    }

    /**
     * Returns the {@link IndexType} of this index descriptor.
     */
    public IndexType getIndexType()
    {
        return schema().getIndexType();
    }

    /**
     * Returns the {@link IndexProviderDescriptor} of the index provider for this index.
     */
    public IndexProviderDescriptor getIndexProvider()
    {
        return indexProvider;
    }

    /**
     * Return the capabilities of this index.
     */
    public IndexCapability getCapability()
    {
        return capability;
    }

    /**
     * Produce a new index descriptor that is the same as this index descriptor in every way, except it has the given index provider descriptor.
     *
     * @param indexProvider The index provider descriptor used in the new index descriptor.
     * @return A new index descriptor with the given index provider.
     */
    public IndexDescriptor2 withIndexProvider( IndexProviderDescriptor indexProvider )
    {
        return new IndexDescriptor2( id, name, schema, isUnique, indexProvider, owningConstraintId, capability );
    }

    /**
     * Produce a new index descriptor that is the same as this index descriptor in every way, except it has the given schema descriptor.
     *
     * @param schema The schema descriptor used in the new index descriptor.
     * @return A new index descriptor with the given schema descriptor.
     */
    public IndexDescriptor2 withSchemaDescriptor( SchemaDescriptor schema )
    {
        return new IndexDescriptor2( id, name, schema, isUnique, indexProvider, owningConstraintId, capability );
    }

    /**
     * Produce a new index descriptor that is the same as this index descriptor in every way, except it has the given owning constraint id.
     *
     * @param owningConstraintId The id of the constraint that owns the index represented by this index descriptor.
     * @return A new index descriptor with the given owning constraint id.
     */
    public IndexDescriptor2 withOwningConstraintId( long owningConstraintId )
    {
        if ( !isUnique() )
        {
            throw new IllegalStateException(
                    "Cannot assign an owning constraint id (in this case " + owningConstraintId + ") to a non-unique index: " + this + "." );
        }
        if ( owningConstraintId < 1 )
        {
            throw new IllegalArgumentException(
                    "The owning constraint id of an index must be positive, but it was attempted to assign " + owningConstraintId + "." );
        }
        return new IndexDescriptor2( id, name, schema, isUnique, indexProvider, owningConstraintId, capability );
    }

    /**
     * Produce a new index descriptor that is the same as this index descriptor in every way, except it has the given index capabilities.
     *
     * @param capability The capabilities of the new index descriptor.
     * @return A new index descriptor with the given capabilities.
     */
    public IndexDescriptor2 withIndexCapability( IndexCapability capability )
    {
        return new IndexDescriptor2( id, name, schema, isUnique, indexProvider, owningConstraintId, capability );
    }
}
