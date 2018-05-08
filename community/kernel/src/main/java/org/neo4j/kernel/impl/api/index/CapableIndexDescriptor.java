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
package org.neo4j.kernel.impl.api.index;

import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexValueCapability;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.PendingIndexDescriptor;
import org.neo4j.values.storable.ValueCategory;

public class CapableIndexDescriptor extends IndexDescriptor
{
    private final IndexCapability indexCapability;

    public CapableIndexDescriptor( long id,
                                   IndexProvider.Descriptor providerDescriptor,
                                   PendingIndexDescriptor descriptor,
                                   Long owningConstraintId,
                                   IndexCapability indexCapability )
    {
        super( id, providerDescriptor, descriptor, owningConstraintId );
        this.indexCapability = indexCapability;
    }

    public CapableIndexDescriptor( IndexDescriptor indexDescriptor, IndexCapability indexCapability )
    {
        super( indexDescriptor.getId(), indexDescriptor.providerDescriptor(), indexDescriptor, indexDescriptor.getOwningConstraint() );
        this.indexCapability = indexCapability;
    }

    @Override
    public String providerKey()
    {
        return providerDescriptor.getKey();
    }

    @Override
    public String providerVersion()
    {
        return providerDescriptor.getVersion();
    }

    @Override
    public IndexOrder[] orderCapability( ValueCategory... valueCategories )
    {
        return indexCapability.orderCapability( valueCategories );
    }

    @Override
    public IndexValueCapability valueCapability( ValueCategory... valueCategories )
    {
        return indexCapability.valueCapability( valueCategories );
    }
}
