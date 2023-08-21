/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.collection;

import static org.neo4j.collection.PrimitiveLongCollections.resourceIterator;

import java.io.IOException;
import java.util.Arrays;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceUtils;

public class PrimitiveLongResourceCollections {
    private static final PrimitiveLongResourceIterator EMPTY = new AbstractPrimitiveLongBaseResourceIterator(null) {
        @Override
        protected boolean fetchNext() {
            return false;
        }
    };

    public static PrimitiveLongResourceIterator emptyIterator() {
        return EMPTY;
    }

    public static PrimitiveLongResourceIterator iterator(Resource resource, final long... items) {
        return resourceIterator(PrimitiveLongCollections.iterator(items), resource);
    }

    public static long count(PrimitiveLongResourceIterator iterator) throws IOException {
        long count = 0;
        try (iterator) {
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
        }
        return count;
    }

    public static PrimitiveLongResourceIterator concat(
            PrimitiveLongResourceIterator... primitiveLongResourceIterators) {
        return concat(Arrays.asList(primitiveLongResourceIterators));
    }

    public static PrimitiveLongResourceIterator concat(
            Iterable<PrimitiveLongResourceIterator> primitiveLongResourceIterators) {
        return new PrimitiveLongConcatenatingResourceIterator(primitiveLongResourceIterators);
    }

    public static long[] asArray(PrimitiveLongResourceIterator iterator) throws IOException {
        try (iterator) {
            return PrimitiveLongCollections.asArray(iterator);
        }
    }

    public abstract static class AbstractPrimitiveLongBaseResourceIterator
            extends PrimitiveLongCollections.AbstractPrimitiveLongBaseIterator
            implements PrimitiveLongResourceIterator {
        private Resource resource;

        public AbstractPrimitiveLongBaseResourceIterator(Resource resource) {
            this.resource = resource;
        }

        @Override
        public void close() {
            if (resource != null) {
                resource.close();
                resource = null;
            }
        }
    }

    private static final class PrimitiveLongConcatenatingResourceIterator
            extends PrimitiveLongCollections.PrimitiveLongConcatenatingIterator
            implements PrimitiveLongResourceIterator {
        private final Iterable<PrimitiveLongResourceIterator> iterators;
        private volatile boolean closed;

        private PrimitiveLongConcatenatingResourceIterator(Iterable<PrimitiveLongResourceIterator> iterators) {
            super(iterators.iterator());
            this.iterators = iterators;
        }

        @Override
        protected boolean fetchNext() {
            return !closed && super.fetchNext();
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                ResourceUtils.closeAll(iterators);
            }
        }
    }
}
