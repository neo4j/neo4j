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

import org.neo4j.common.TokenNameLookup;

public interface IndexDescriptor extends SchemaDescriptorSupplier
{
    /**
     * Returns true if this index only allows one value per key.
     */
    boolean isUnique();

    /**
     * @return whether or not this descriptor has a user-specified {@link #name()}. Regardless the {@link #name()} method will
     * return some name, at the very least an automatically generated one.
     */
    boolean hasUserSuppliedName();

    /**
     * The unique name for this index - either automatically generated or user supplied at creation.
     */
    String name();

    /**
     * Returns a user friendly description of what this index indexes.
     *
     * @param tokenNameLookup used for looking up names for token ids.
     * @return a user friendly description of what this index indexes.
     */
    String userDescription( TokenNameLookup tokenNameLookup );

    boolean isFulltextIndex();

    boolean isEventuallyConsistent();

    String providerKey();

    String providerVersion();
}
