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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.ValueMerger;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;

/**
 * {@link ValueMerger} which will merely detect conflict, not change any value if conflict, i.e. if the
 * key already exists. After this merge has been used in a call to {@link Writer#merge(Object, Object, ValueMerger)}
 * {@link #checkConflict(Value[])} can be called to check whether or not that call conflicted with
 * an existing key. A call to {@link #checkConflict(Value[])} will also initialize the conflict flag.
 *
 * @param <VALUE> type of values being merged.
 */
class ConflictDetectingValueMerger<KEY extends NativeSchemaKey<KEY>, VALUE extends NativeSchemaValue> implements ValueMerger<KEY,VALUE>
{
    private final boolean compareEntityIds;

    private boolean conflict;
    private long existingNodeId;
    private long addedNodeId;

    ConflictDetectingValueMerger( boolean compareEntityIds )
    {
        this.compareEntityIds = compareEntityIds;
    }

    @Override
    public VALUE merge( KEY existingKey, KEY newKey, VALUE existingValue, VALUE newValue )
    {
        if ( existingKey.getEntityId() != newKey.getEntityId() )
        {
            conflict = true;
            existingNodeId = existingKey.getEntityId();
            addedNodeId = newKey.getEntityId();
        }
        return null;
    }

    /**
     * To be called for a populated key that is about to be sent off to a {@link Writer}.
     * {@link GBPTree}'s ability to check for conflicts while applying updates is an opportunity,
     * but also complicates some scenarios. This is why the strictness can be tweaked like this.
     *
     * @param key key to let know about conflict detection strictness.
     */
    void controlConflictDetection( KEY key )
    {
        key.setCompareId( compareEntityIds );
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
