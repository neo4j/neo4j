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
package org.neo4j.consistency.checking.cache;

import static java.lang.Math.ceil;

public interface CacheSlots
{
    int LABELS_SLOT_SIZE = 40;
    int ID_SLOT_SIZE = 40;
    long MAX_ID_SLOT_SIZE = 1L << ID_SLOT_SIZE - 1;
    long CACHE_LINE_SIZE_BYTES = (long) ceil( (2D * ID_SLOT_SIZE + 3) / Byte.SIZE );

    interface NodeLabel
    {
        int SLOT_LABEL_FIELD = 0;
        int SLOT_IN_USE = 1;
    }

    interface NextRelationship
    {
        int SLOT_RELATIONSHIP_ID = 0;
        int SLOT_FIRST_IN_SOURCE = 1;
        int SLOT_FIRST_IN_TARGET = 2;
        int SLOT_NODE_IS_DENSE = 3;
        int SLOT_CHECK_MARK = 4;
    }

    interface NodeLink
    {
        int SLOT_RELATIONSHIP_ID = 0;
        int SLOT_LABELS = 1;
        int SLOT_IN_USE = 2;
        int SLOT_IS_DENSE = 3;
        int SLOT_HAS_INLINED_LABELS = 4;
        int SLOT_CHECK_MARK = 5;
        int SLOT_HAS_SINGLE_LABEL = 6;
    }

    interface RelationshipLink
    {
        int SLOT_RELATIONSHIP_ID = 0;
        int SLOT_REFERENCE = 1;
        int SLOT_SOURCE_OR_TARGET = 2;
        int SLOT_PREV_OR_NEXT = 3;
        int SLOT_IN_USE = 4;
        int SLOT_HAS_MULTIPLE_RELATIONSHIPS = 5;
        int SLOT_FIRST_IN_CHAIN = 6;
        long SOURCE = 0;
        long TARGET = -1;
        long PREV = 0;
        long NEXT = -1;
    }

    static long longOf( boolean value )
    {
        return value ? 1 : 0;
    }
}
