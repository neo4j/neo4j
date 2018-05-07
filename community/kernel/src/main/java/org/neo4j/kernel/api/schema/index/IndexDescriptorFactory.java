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
package org.neo4j.kernel.api.schema.index;


import java.util.Optional;

import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;

import static org.neo4j.kernel.api.schema.index.PendingIndexDescriptor.Type.GENERAL;
import static org.neo4j.kernel.api.schema.index.PendingIndexDescriptor.Type.UNIQUE;

public class IndexDescriptorFactory
{
    private IndexDescriptorFactory()
    {
    }

    public static PendingIndexDescriptor forLabel( int labelId, int... propertyIds )
    {
        return forSchema( SchemaDescriptorFactory.forLabel( labelId, propertyIds ) );
    }

    public static PendingIndexDescriptor uniqueForLabel( int labelId, int... propertyIds )
    {
        return uniqueForSchema( SchemaDescriptorFactory.forLabel( labelId, propertyIds ) );
    }

    public static PendingIndexDescriptor forSchema( SchemaDescriptor schema )
    {
        return new PendingIndexDescriptor( schema, GENERAL, null, null );
    }

    public static PendingIndexDescriptor forSchema( SchemaDescriptor schema,
                                                    Optional<String> name,
                                                    IndexProvider.Descriptor providerDescriptor )
    {
        return new PendingIndexDescriptor( schema, GENERAL, name, providerDescriptor );
    }

    public static PendingIndexDescriptor uniqueForSchema( SchemaDescriptor schema )
    {
        return new PendingIndexDescriptor( schema, UNIQUE, null, null );
    }
}
