/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util.Optional;

import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.values.storable.Value;

public interface IndexingProvidersService
{
    /**
     * Get the index provider descriptor for the index provider with the given name, or the
     * descriptor of the default index provider, if no name was given.
     *
     * @param providerName name of the wanted index provider
     */
    IndexProviderDescriptor indexProviderForNameOrDefault( Optional<String> providerName );

    /**
     * Validate that the given value tuple can be stored in the index associated with the given schema.
     *
     * @param schema index schema of the target index
     * @param tuple value tuple to validate
     */
    void validateBeforeCommit( SchemaDescriptor schema, Value[] tuple );
}
