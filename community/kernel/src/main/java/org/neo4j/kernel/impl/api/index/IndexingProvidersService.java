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
package org.neo4j.kernel.impl.api.index;

import org.neo4j.internal.kernel.api.exceptions.schema.MisconfiguredIndexException;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.values.storable.Value;

public interface IndexingProvidersService
{
    /**
     * Get the index provider descriptor for the index provider with the given name, or the
     * descriptor of the default index provider, if no name was given.
     *
     * @param providerName name of the wanted index provider
     */
    IndexProviderDescriptor indexProviderByName( String providerName );

    /**
     * Validate that the given value tuple can be stored in the index associated with the given schema.
     *
     * @param schema index schema of the target index
     * @param tuple value tuple to validate
     */
    void validateBeforeCommit( SchemaDescriptor schema, Value[] tuple );

    /**
     * Since indexes can now have provider-specific settings and configurations, the provider needs to have an opportunity to inspect and validate the index
     * descriptor before an index is created. The return descriptor is a blessed version of the given descriptor, and is what must be used for creating an
     * index.
     * @param index The descriptor of an index that we are about to create, and we wish to be blessed by its chosen index provider.
     * @return The blessed index descriptor.
     * @throws MisconfiguredIndexException if the provider cannot be bless the given index descriptor.
     */
    IndexDescriptor getBlessedDescriptorFromProvider( IndexDescriptor index ) throws MisconfiguredIndexException;
}
