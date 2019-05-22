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

import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.IndexValueCapability;
import org.neo4j.internal.schema.DefaultIndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.values.storable.ValueCategory;

/**
 * Internal representation of a graph index, including the schema unit it targets (eg. label-property combination)
 * and the type of index. UNIQUE indexes are used to back uniqueness constraints.
 * <p>
 * An IndexDescriptor might represent an index that has not yet been committed, and therefore carries an optional
 * user-supplied name. On commit the descriptor is upgraded to a {@link StoreIndexDescriptor} using
 * {@link IndexDescriptor#withId(long)} or {@link IndexDescriptor#withIds(long, long)}.
 *
 * This class extends {@link DefaultIndexDescriptor} just to cut down on the code duplication of implementing these methods,
 * it doesn't <strong>have to</strong> extend it.
 */
public class IndexDescriptor extends DefaultIndexDescriptor
        implements SchemaDescriptorSupplier, IndexReference, org.neo4j.internal.schema.IndexDescriptor
{
    protected final IndexProviderDescriptor providerDescriptor;

    public IndexDescriptor( org.neo4j.internal.schema.IndexDescriptor indexDescriptor )
    {
        super( indexDescriptor );
        this.providerDescriptor = IndexProviderDescriptor.from( indexDescriptor );
    }

    public IndexDescriptor( SchemaDescriptor schema, boolean isUnique, Optional<String> userSuppliedName, IndexProviderDescriptor providerDescriptor )
    {
        super( schema, providerDescriptor.getKey(), providerDescriptor.getVersion(), userSuppliedName, isUnique, false );
        this.providerDescriptor = providerDescriptor;
    }

    protected IndexDescriptor( DefaultIndexDescriptor descriptor, IndexProviderDescriptor providerDescriptor )
    {
        super( descriptor );
        this.providerDescriptor = providerDescriptor;
    }

    // METHODS

    @Override
    public int[] properties()
    {
        return schema().getPropertyIds();
    }

    @Override
    public String name()
    {
        return name.orElse( UNNAMED_INDEX );
    }

    @Override
    public IndexDescriptor withIndexProvider( IndexProviderDescriptor indexProvider )
    {
        return new IndexDescriptor( super.withIndexProvider( indexProvider ), indexProvider );
    }

    @Override
    public IndexDescriptor withSchemaDescriptor( SchemaDescriptor schema )
    {
        return new IndexDescriptor( super.withSchemaDescriptor( schema ), providerDescriptor );
    }

    @Override
    public IndexDescriptor withEventualConsistency( boolean isEventuallyConsistent )
    {
        return new IndexDescriptor( super.withEventualConsistency( isEventuallyConsistent ), providerDescriptor );
    }

    public IndexProviderDescriptor providerDescriptor()
    {
        return providerDescriptor;
    }

    @Override
    public IndexOrder[] orderCapability( ValueCategory... valueCategories )
    {
        return ORDER_NONE;
    }

    @Override
    public IndexValueCapability valueCapability( ValueCategory... valueCategories )
    {
        return IndexValueCapability.NO;
    }

    /**
     * Create a StoreIndexDescriptor, which represent the commit version of this index.
     *
     * @param id the index id of the committed index
     * @return a StoreIndexDescriptor
     */
    public StoreIndexDescriptor withId( long id )
    {
        assertValidId( id, "id" );
        return new StoreIndexDescriptor( this, id );
    }

    /**
     * Create a StoreIndexDescriptor, which represent the commit version of this index, that is owned
     * by a uniqueness constraint.
     *
     * @param id id of the committed index
     * @param owningConstraintId id of the uniqueness constraint owning this index
     * @return a StoreIndexDescriptor
     */
    public StoreIndexDescriptor withIds( long id, long owningConstraintId )
    {
        assertValidId( id, "id" );
        assertValidId( owningConstraintId, "owning constraint id" );
        return new StoreIndexDescriptor( this, id, owningConstraintId );
    }

    void assertValidId( long id, String idName )
    {
        if ( id < 0 )
        {
            throw new IllegalArgumentException( "A " + getClass().getSimpleName() + " " + idName + " must be positive, got " + id );
        }
    }
}
