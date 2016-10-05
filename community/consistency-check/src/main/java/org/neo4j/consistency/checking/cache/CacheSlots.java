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
package org.neo4j.consistency.checking.cache;

public interface CacheSlots
{
    int LABELS_SLOT_SIZE = 63;
    int ID_SLOT_SIZE = 35;

    interface NodeLabel
    {
        int SLOT_IN_USE = 0;
        int SLOT_LABEL_FIELD = 1;
    }

    interface NextRelationhip
    {
        int SLOT_FIRST_IN_SOURCE = 0;
        int SLOT_FIRST_IN_TARGET = 1;
        int SLOT_RELATIONSHIP_ID = 2;
    }

    interface RelationshipLink
    {
        int SLOT_SOURCE_OR_TARGET = 0;
        int SLOT_PREV_OR_NEXT = 1;
        int SLOT_RELATIONSHIP_ID = 2;
        int SLOT_REFERENCE = 3;
        long SOURCE = 0;
        long TARGET = -1;
        long PREV = 0;
        long NEXT = -1;
    }
}
