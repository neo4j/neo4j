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

/**
 * Something that can complete the configuration of index descriptors, by adding any missing configurations, and assigning them capabilities.
 */
public interface IndexConfigCompleter
{
    /**
     * Since indexes can now have provider-specific settings and configurations, the provider needs to have an opportunity to inspect and validate the index
     * descriptor before an index is created. The provider also uses this opportunity to assign capabilities to the index.
     * The returned descriptor is a version of the given descriptor which has a fully fleshed out configuration, and is what must be used for creating an index.
     * <p>
     * Note that this is an additive and idempotent operation. If an index is already configured, then this method must not overwrite the existing
     * configuration.
     *
     * @param index The descriptor of an index that we are about to create, and we wish to its configuration be completed by its chosen index provider.
     * @return An index descriptor with a completed configuration.
     */
    IndexDescriptor completeConfiguration( IndexDescriptor index );
}
