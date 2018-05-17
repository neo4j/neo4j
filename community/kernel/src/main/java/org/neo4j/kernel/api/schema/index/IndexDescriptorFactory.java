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

import static org.neo4j.kernel.api.schema.index.IndexDescriptor.Type.GENERAL;
import static org.neo4j.kernel.api.schema.index.IndexDescriptor.Type.UNIQUE;

public class IndexDescriptorFactory
{
    private IndexDescriptorFactory()
    {
    }

    public static IndexDescriptor forSchema( SchemaDescriptor schema )
    {
        return new IndexDescriptor( schema, GENERAL, Optional.empty(), IndexProvider.UNDECIDED );
    }

    public static IndexDescriptor forSchema( SchemaDescriptor schema,
                                             Optional<String> name,
                                             IndexProvider.Descriptor providerDescriptor )
    {
        return new IndexDescriptor( schema, GENERAL, name, providerDescriptor );
    }

    public static IndexDescriptor forSchema( SchemaDescriptor schema,
                                             IndexProvider.Descriptor providerDescriptor )
    {
        return new IndexDescriptor( schema, GENERAL, Optional.empty(), providerDescriptor );
    }

    public static IndexDescriptor uniqueForSchema( SchemaDescriptor schema )
    {
        return new IndexDescriptor( schema, UNIQUE, Optional.empty(), IndexProvider.UNDECIDED );
    }

    public static IndexDescriptor uniqueForSchema( SchemaDescriptor schema,
                                                   IndexProvider.Descriptor providerDescriptor )
    {
        return new IndexDescriptor( schema, UNIQUE, Optional.empty(), providerDescriptor );
    }

    public static IndexDescriptor uniqueForSchema( SchemaDescriptor schema,
                                                   Optional<String> name,
                                                   IndexProvider.Descriptor providerDescriptor )
    {
        return new IndexDescriptor( schema, UNIQUE, name, providerDescriptor );
    }
}
