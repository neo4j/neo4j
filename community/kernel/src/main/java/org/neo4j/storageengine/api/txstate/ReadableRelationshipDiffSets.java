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

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

/**
 * {@link SuperReadableDiffSets} with added method for filtering added relationships.
 */
public interface ReadableRelationshipDiffSets<T> extends SuperReadableDiffSets<T>
{
    @Override
    ReadableRelationshipDiffSets<T> filterAdded( Predicate<T> addedFilter );

    RelationshipIterator augment( RelationshipIterator source );

    @Override
    default PrimitiveLongIterator augment( PrimitiveLongIterator source )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    default PrimitiveLongResourceIterator augment( PrimitiveLongResourceIterator source )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    default PrimitiveLongResourceIterator augmentWithRemovals( PrimitiveLongResourceIterator source )
    {
        throw new UnsupportedOperationException();
    }

    final class Empty<T> implements ReadableRelationshipDiffSets<T>
    {
        @SuppressWarnings( "unchecked" )
        public static <T> ReadableRelationshipDiffSets<T> ifNull( ReadableRelationshipDiffSets<T> diffSets )
        {
            return diffSets == null ? INSTANCE : diffSets;
        }

        @SuppressWarnings( "rawtypes" )
        private static final ReadableRelationshipDiffSets INSTANCE = new Empty();

        private Empty()
        {
            // singleton
        }

        @Override
        public boolean isAdded( T elem )
        {
            return false;
        }

        @Override
        public boolean isRemoved( T elem )
        {
            return false;
        }

        @Override
        public Set<T> getAdded()
        {
            return Collections.emptySet();
        }

        @Override
        public Set<T> getAddedSnapshot()
        {
            return Collections.emptySet();
        }

        @Override
        public Set<T> getRemoved()
        {
            return Collections.emptySet();
        }

        @Override
        public boolean isEmpty()
        {
            return true;
        }

        @Override
        public Iterator<T> apply( Iterator<T> source )
        {
            return source;
        }

        @Override
        public int delta()
        {
            return 0;
        }

        @Override
        public RelationshipIterator augment( RelationshipIterator source )
        {
            return source;
        }

        @Override
        public ReadableRelationshipDiffSets<T> filterAdded( Predicate<T> addedFilter )
        {
            return this;
        }

        @Override
        public void accept( DiffSetsVisitor<T> visitor )
        {
        }
    }
}
