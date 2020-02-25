/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

public enum IndexBehaviour
{
    /**
     * Highlights that CONTAINS and ENDS WITH isn't supported efficiently.
     */
    SLOW_CONTAINS,
    /**
     * The index is <em>eventually consistent</em>, meaning it might not reflect newly committed changes.
     */
    EVENTUALLY_CONSISTENT,
    /**
     * The index has optimised support for SKIP and LIMIT, allowing these predicates to be pushed down.
     */
    SKIP_AND_LIMIT,
}
