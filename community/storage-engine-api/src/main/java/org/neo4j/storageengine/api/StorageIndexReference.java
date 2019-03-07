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

import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaRule;

public interface StorageIndexReference extends IndexDescriptor, SchemaRule
{
    /**
     * @return reference to this index.
     */
    long indexReference();

    /**
     * @return whether or not this index has an owning constraint. This method is only valid to call if this index is {@link #isUnique() unique}.
     */
    boolean hasOwningConstraintReference();

    /**
     * @return reference to the owning constraint, if present.
     * @throws IllegalStateException if this isn't a {@link #isUnique() unique} index or if this index doesn't have an owning constraint.
     */
    long owningConstraintReference();
}
