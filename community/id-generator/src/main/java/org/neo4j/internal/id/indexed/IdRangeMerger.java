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
package org.neo4j.internal.id.indexed;

import org.neo4j.index.internal.gbptree.ValueMerger;

import static org.neo4j.index.internal.gbptree.ValueMerger.MergeResult.MERGED;
import static org.neo4j.index.internal.gbptree.ValueMerger.MergeResult.REMOVED;
import static org.neo4j.index.internal.gbptree.ValueMerger.MergeResult.UNCHANGED;

/**
 * Merges ID state changes for a particular tree entry. Differentiates between recovery/normal mode.
 * Updates to a tree entry of an older generation during normal mode will first normalize states before applying new changes.
 */
final class IdRangeMerger implements ValueMerger<IdRangeKey, IdRange>
{
    public static final IdRangeMerger DEFAULT = new IdRangeMerger( false );
    public static final IdRangeMerger RECOVERY = new IdRangeMerger( true );

    private final boolean recoveryMode;

    private IdRangeMerger( boolean recoveryMode )
    {
        this.recoveryMode = recoveryMode;
    }

    @Override
    public MergeResult merge( IdRangeKey existingKey, IdRangeKey newKey, IdRange existingValue, IdRange newValue )
    {
        if ( !recoveryMode && existingValue.getGeneration() != newValue.getGeneration() )
        {
            existingValue.normalize();
            existingValue.setGeneration( newValue.getGeneration() );
        }

        final boolean changed = existingValue.mergeFrom( newValue, recoveryMode );
        if ( !changed )
        {
            return UNCHANGED;
        }

        return existingValue.isEmpty() ? REMOVED : MERGED;
    }
}
