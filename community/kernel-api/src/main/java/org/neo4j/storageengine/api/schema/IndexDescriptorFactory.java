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
package org.neo4j.storageengine.api.schema;

import java.util.Optional;

import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;

import static org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor.UNDECIDED;
import static org.neo4j.storageengine.api.schema.IndexDescriptor.Type.GENERAL;
import static org.neo4j.storageengine.api.schema.IndexDescriptor.Type.UNIQUE;

public class IndexDescriptorFactory
{
    private IndexDescriptorFactory()
    {
    }

    public static IndexDescriptor forSchema( SchemaDescriptor schema )
    {
        return forSchema( schema, UNDECIDED );
    }

    public static IndexDescriptor forSchema( SchemaDescriptor schema,
                                             IndexProviderDescriptor providerDescriptor )
    {
        return forSchema( schema, Optional.empty(), providerDescriptor );
    }

    public static IndexDescriptor forSchema( SchemaDescriptor schema,
                                             Optional<String> name,
                                             IndexProviderDescriptor providerDescriptor )
    {
        return new IndexDescriptor( schema, GENERAL, name, providerDescriptor );
    }

    public static IndexDescriptor uniqueForSchema( SchemaDescriptor schema )
    {
        return uniqueForSchema( schema, UNDECIDED );
    }

    public static IndexDescriptor uniqueForSchema( SchemaDescriptor schema,
                                                   IndexProviderDescriptor providerDescriptor )
    {
        return uniqueForSchema( schema,Optional.empty(), providerDescriptor );
    }

    public static IndexDescriptor uniqueForSchema( SchemaDescriptor schema,
                                                   Optional<String> name,
                                                   IndexProviderDescriptor providerDescriptor )
    {
        return new IndexDescriptor( schema, UNIQUE, name, providerDescriptor );
    }
}
