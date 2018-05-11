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
package org.neo4j.kernel.impl.api.store;

import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexValueCapability;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.values.storable.ValueCategory;

public class DefaultCapableIndexReference extends DefaultIndexReference implements CapableIndexReference
{
    private final IndexProvider.Descriptor providerDescriptor;
    private final IndexCapability capability;

    public DefaultCapableIndexReference( boolean unique, IndexCapability indexCapability,
            IndexProvider.Descriptor providerDescriptor, int label, int... properties )
    {
        super( unique, label, properties );
        this.capability = indexCapability;
        this.providerDescriptor = providerDescriptor;
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
        return capability.orderCapability( valueCategories );
    }

    @Override
    public IndexValueCapability valueCapability( ValueCategory... valueCategories )
    {
        return capability.valueCapability( valueCategories );
    }

    public static CapableIndexReference fromDescriptor( SchemaIndexDescriptor descriptor )
    {
        boolean unique = descriptor.type() == SchemaIndexDescriptor.Type.UNIQUE;
        final SchemaDescriptor schema = descriptor.schema();
        return new DefaultCapableIndexReference( unique, IndexCapability.NO_CAPABILITY, IndexProvider.UNDECIDED,
                schema.keyId(), schema.getPropertyIds() );
    }
}
