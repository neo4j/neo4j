/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.ValueMerger;
import org.neo4j.index.internal.gbptree.ValueMergers;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;

/**
 * {@link ValueMerger} which will merely detect conflict, not change any value if conflict, i.e. if the
 * key already exists. After this merge has been used in a call to {@link Writer#merge(Object, Object, ValueMerger)}
 * {@link #checkConflict(Value[])} can be called to check whether or not that call conflicted with
 * an existing key. A call to {@link #checkConflict(Value[])} will also clear the conflict flag.
 *
 * @param <VALUE> type of values being merged.
 */
abstract class ConflictDetectingValueMerger<KEY extends SchemaNumberKey, VALUE extends SchemaNumberValue> implements ValueMerger<KEY,VALUE>
{
    /**
     * @throw IndexEntryConflictException if merge conflicted with an existing key. This call also clears the conflict flag.
     */
    abstract void checkConflict( Value[] values ) throws IndexEntryConflictException;

    private static ConflictDetectingValueMerger DONT_CHECK = new ConflictDetectingValueMerger()
    {
        @Override
        public Object merge( Object existingKey, Object newKey, Object existingValue, Object newValue )
        {
            return null;
        }

        @Override
        void checkConflict( Value[] values )
        {
            // do nothing
        }
    };

    static <KEY extends SchemaNumberKey, VALUE extends SchemaNumberValue> ConflictDetectingValueMerger<KEY, VALUE> dontCheck()
    {
        return DONT_CHECK;
    }

    static class Check<KEY extends SchemaNumberKey, VALUE extends SchemaNumberValue> extends ConflictDetectingValueMerger<KEY, VALUE>
    {
        private boolean conflict;
        private long existingNodeId;
        private long addedNodeId;

        @Override
        public VALUE merge( KEY existingKey, KEY newKey, VALUE existingValue, VALUE newValue )
        {
            if ( existingKey.entityId != newKey.entityId )
            {
                conflict = true;
                existingNodeId = existingKey.entityId;
                addedNodeId = newKey.entityId;
            }
            return null;
        }

        void checkConflict( Value[] values ) throws IndexEntryConflictException
        {
            if ( conflict )
            {
                conflict = false;
                throw new IndexEntryConflictException( existingNodeId, addedNodeId, ValueTuple.of( values ) );
            }
        }
    }
}
