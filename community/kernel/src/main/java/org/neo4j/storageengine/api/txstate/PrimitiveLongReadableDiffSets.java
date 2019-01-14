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
package org.neo4j.storageengine.api.txstate;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.emptySet;

/**
 * Read only variant of specialised primitive longs collection that with given a sequence of add
 * and removal operations, tracks which elements need to actually be added and removed at minimum from some
 * target collection such that the result is equivalent to just
 * executing the sequence of additions and removals in order.
 */
public interface PrimitiveLongReadableDiffSets
{
    PrimitiveLongReadableDiffSets EMPTY = new PrimitiveLongReadableDiffSets()
    {
        @Override
        public boolean isAdded( long element )
        {
            return false;
        }

        @Override
        public boolean isRemoved( long element )
        {
            return false;
        }

        @Override
        public PrimitiveLongSet getAdded()
        {
            return emptySet();
        }

        @Override
        public PrimitiveLongSet getAddedSnapshot()
        {
            return emptySet();
        }

        @Override
        public PrimitiveLongSet getRemoved()
        {
            return emptySet();
        }

        @Override
        public boolean isEmpty()
        {
            return true;
        }

        @Override
        public int delta()
        {
            return 0;
        }

        @Override
        public PrimitiveLongIterator augment( PrimitiveLongIterator elements )
        {
            return elements;
        }

        @Override
        public PrimitiveLongResourceIterator augment( PrimitiveLongResourceIterator elements )
        {
            return elements;
        }
    };

    /**
     * Check if provided element added in this collection
     * @param element element to check
     * @return true if added, false otherwise
     */
    boolean isAdded( long element );

    /**
     * Check if provided element is removed in this collection
     * @param element element to check
     * @return true if removed, false otherwise
     */
    boolean isRemoved( long element );

    /**
     * All elements that added into this collection
     * @return all added elements
     */
    PrimitiveLongSet getAdded();

    /**
     * Snapshot of added elements infot this collection on the moment of invocation
     * @return snapshot of added elements
     */
    PrimitiveLongSet getAddedSnapshot();

    /**
     * All elements that are removed according to underlying collection
     * @return all removed elements
     */
    PrimitiveLongSet getRemoved();

    /**
     * Check if underlying diff set is empty
     * @return true if there is no added and removed elements, false otherwise
     */
    boolean isEmpty();

    /**
     * Difference between number of added and removed elements
     * @return difference between number of added and removed elements
     */
    int delta();

    /**
     * Augment current diff sets with elements. Provided element will be augmented if diffset
     * does not remove and add that specific element.
     * @param elements elements to augment with
     * @return iterator that will iterate over augmented elements as well as over diff set
     */
    PrimitiveLongIterator augment( PrimitiveLongIterator elements );

    /**
     * Augment current diff sets with elements. Provided element will be augmented if diffset
     * does not remove and add that specific element.
     *
     * @param elements elements to augment with
     * @return iterator that will iterate over augmented elements as well as over diff set
     */
    PrimitiveLongResourceIterator augment( PrimitiveLongResourceIterator elements );
}
