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
 * Enum used for two purposes:
 * 1. As return value for {@link IndexCapability#orderCapability(org.neo4j.values.storable.ValueCategory...)}.
 * Only {@link #ASCENDING} and {@link #DESCENDING} is valid for this.
 * 2. As parameter for {@link Read#nodeIndexScan(IndexReference, NodeValueIndexCursor, IndexOrder)} and
 * {@link Read#nodeIndexSeek(IndexReference, NodeValueIndexCursor, IndexOrder, IndexQuery...)}. Where {@link #NONE} is used when
 * no ordering is available or required.
 */
public enum IndexOrder
{
    ASCENDING, DESCENDING, NONE
}
