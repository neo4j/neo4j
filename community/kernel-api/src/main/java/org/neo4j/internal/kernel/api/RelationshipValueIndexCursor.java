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
package org.neo4j.internal.kernel.api;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;
import static org.neo4j.storageengine.api.LongReference.NULL_REFERENCE;
import static org.neo4j.values.storable.Values.NO_VALUE;

import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.values.storable.Value;

/**
 * Cursor for scanning the property values of relationships in a schema index.
 * <p>
 * Usage pattern:
 * <pre><code>
 *     int nbrOfProps = cursor.numberOfProperties();
 *
 *     Value[] values = new Value[nbrOfProps];
 *     while ( cursor.next() )
 *     {
 *         if ( cursor.hasValue() )
 *         {
 *             for ( int i = 0; i < nbrOfProps; i++ )
 *             {
 *                 values[i] = cursor.propertyValue( i );
 *             }
 *         }
 *         else
 *         {
 *             values[i] = getPropertyValueFromStore( cursor.relationshipReference(), cursor.propertyKey( i ) )
 *         }
 *
 *         doWhatYouWantToDoWith( values );
 *     }
 * </code></pre>
 */
public interface RelationshipValueIndexCursor extends RelationshipIndexCursor, ValueIndexCursor {
    class Empty extends DoNothingCloseListenable implements RelationshipValueIndexCursor {
        @Override
        public void source(NodeCursor cursor) {}

        @Override
        public void target(NodeCursor cursor) {}

        @Override
        public int type() {
            return NO_SUCH_RELATIONSHIP_TYPE;
        }

        @Override
        public long sourceNodeReference() {
            return NO_SUCH_NODE;
        }

        @Override
        public long targetNodeReference() {
            return NO_SUCH_NODE;
        }

        @Override
        public long relationshipReference() {
            return NO_SUCH_RELATIONSHIP;
        }

        @Override
        public boolean next() {
            return false;
        }

        @Override
        public boolean readFromStore() {
            return false;
        }

        @Override
        public void setTracer(KernelReadTracer tracer) {}

        @Override
        public void removeTracer() {}

        @Override
        public void closeInternal() {}

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public float score() {
            return Float.NaN;
        }

        @Override
        public int numberOfProperties() {
            return 0;
        }

        @Override
        public boolean hasValue() {
            return false;
        }

        @Override
        public Value propertyValue(int offset) {
            return NO_VALUE;
        }

        @Override
        public void properties(PropertyCursor cursor, PropertySelection selection) {}

        @Override
        public Reference propertiesReference() {
            return NULL_REFERENCE;
        }
    }

    RelationshipValueIndexCursor EMPTY = new Empty();
}
