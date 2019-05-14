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
package org.neo4j.storageengine.api;

import java.util.Optional;

import org.neo4j.internal.schema.DefaultIndexDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;

/**
 * Default implementation of a {@link StorageIndexReference}. Mainly used as data carrier between the storage engine API and kernel.
 */
public final class DefaultStorageIndexReference extends DefaultIndexDescriptor implements StorageIndexReference
{
    private final long indexReference;
    private final Long owningConstraintReference;

    /**
     * Main constructor.
     */
    public DefaultStorageIndexReference(
            SchemaDescriptor schema,
            String providerKey,
            String providerVersion,
            long indexReference,
            Optional<String> name,
            boolean isUnique,
            Long owningConstraintReference )
    {
        super( schema, providerKey, providerVersion, name, isUnique );
        this.indexReference = indexReference;
        this.owningConstraintReference = owningConstraintReference;
    }

    /**
     * Constructor used for tests when we don't really care about index provider, name and eventual consistency.
     */
    public DefaultStorageIndexReference( SchemaDescriptor schema, boolean isUnique, long indexReference, Long owningConstraintReference )
    {
        super( schema, isUnique );
        this.indexReference = indexReference;
        this.owningConstraintReference = owningConstraintReference;
    }

    /**
     * Copy-constructor that set index reference and owning constraint reference.
     * Typically for when storing this {@link SchemaRule} for the first time.
     */
    public DefaultStorageIndexReference( IndexDescriptor index, long indexReference, Long owningConstraintRef )
    {
        this( index.schema(), index.providerKey(), index.providerVersion(), indexReference, optionalName( index ), index.isUnique(), owningConstraintRef );
    }

    /**
     * Copy-constructor that set owning constraint reference.
     * Typically for when updating populated uniqueness index with an owning constraint.
     */
    public DefaultStorageIndexReference( StorageIndexReference index, long owningConstraintReference )
    {
        this( index.schema(), index.providerKey(), index.providerVersion(), index.indexReference(),
                optionalName( index ), index.isUnique(), owningConstraintReference );
    }

    protected DefaultStorageIndexReference( DefaultIndexDescriptor descriptor, long indexReference, Long owningConstraintReference )
    {
        super( descriptor );
        this.indexReference = indexReference;
        this.owningConstraintReference = owningConstraintReference;
    }

    @Override
    public long indexReference()
    {
        return indexReference;
    }

    @Override
    public boolean hasOwningConstraintReference()
    {
        assertUniqueTypeIndex();
        return owningConstraintReference != null;
    }

    @Override
    public long owningConstraintReference()
    {
        assertUniqueTypeIndex();
        assertHasOwningConstraint();
        return owningConstraintReference;
    }

    @Override
    public DefaultStorageIndexReference withIndexProvider( IndexProviderDescriptor indexProvider )
    {
        return new DefaultStorageIndexReference( super.withIndexProvider( indexProvider ), indexReference, owningConstraintReference );
    }

    @Override
    public DefaultStorageIndexReference withSchemaDescriptor( SchemaDescriptor schema )
    {
        return new DefaultStorageIndexReference( super.withSchemaDescriptor( schema ), indexReference, owningConstraintReference );
    }

    @Override
    public String name()
    {
        return name.orElse( "index_" + indexReference );
    }

    private void assertUniqueTypeIndex()
    {
        if ( !isUnique() )
        {
            throw new IllegalStateException( "Can only get owner from constraint indexes." );
        }
    }

    private void assertHasOwningConstraint()
    {
        if ( !hasOwningConstraintReference() )
        {
            throw new IllegalStateException( "No owning constraint for this descriptor" );
        }
    }

    @Override
    public long getId()
    {
        return indexReference;
    }
}
