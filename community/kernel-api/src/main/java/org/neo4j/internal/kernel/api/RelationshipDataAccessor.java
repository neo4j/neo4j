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
package org.neo4j.internal.kernel.api;

/**
 * Surface for accessing relationship data.
 */
public interface RelationshipDataAccessor
{
    long relationshipReference(); // not sure relationships will have independent references,
    // so exposing this might be leakage.

    int type();

    boolean hasProperties();

    void source( NodeCursor cursor );

    void target( NodeCursor cursor );

    void properties( PropertyCursor cursor );

    long sourceNodeReference();

    long targetNodeReference();

    long propertiesReference();
}
