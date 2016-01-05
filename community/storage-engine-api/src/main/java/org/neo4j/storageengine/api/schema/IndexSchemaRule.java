/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.storageengine.api.schema;

import org.neo4j.kernel.api.index.IndexDescriptor;

/**
 * Basically {@link IndexDescriptor} with access to storage data, such as id and ownder.
 */
public interface IndexSchemaRule extends SchemaRule
{
    /**
     * @return property key token id this index is associated to.
     */
    int getPropertyKey();

    /**
     * @return whether or not this index is related to a uniqueness constraint.
     */
    boolean isConstraintIndex();

    /**
     * @return schema rule id of uniqueness constraint owning this index, or {@code null} if not related
     * to a constraint.
     */
    Long getOwningConstraint();
}
