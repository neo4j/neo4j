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
package org.neo4j.internal.counts;

import org.neo4j.index.internal.gbptree.ValueMerger;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.util.concurrent.OutOfOrderSequence;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.neo4j.index.internal.gbptree.ValueMerger.MergeResult.REMOVED;
import static org.neo4j.index.internal.gbptree.ValueMerger.MergeResult.REPLACED;
import static org.neo4j.io.IOUtils.closeAllUnchecked;

/**
 * Writes counts directly into the tree.
 */
class TreeWriter implements CountUpdater.CountWriter
{
    private static final ValueMerger<CountsKey,CountsValue> MERGER =
            ( existingKey, newKey, existingValue, newValue ) -> newValue.count > 0 ? REPLACED : REMOVED;

    private final Writer<CountsKey,CountsValue> treeWriter;
    private final OutOfOrderSequence idSequence;
    private final long txId;
    private final CountsValue value = new CountsValue();

    TreeWriter( Writer<CountsKey,CountsValue> treeWriter, OutOfOrderSequence idSequence, long txId )
    {
        this.treeWriter = treeWriter;
        this.idSequence = idSequence;
        this.txId = txId;
    }

    @Override
    public void write( CountsKey key, long delta )
    {
        merge( treeWriter, key, value.initialize( delta ) );
    }

    @Override
    public void close()
    {
        closeAllUnchecked( treeWriter );
        idSequence.set( txId, EMPTY_LONG_ARRAY );
    }

    static void merge( Writer<CountsKey,CountsValue> writer, CountsKey key, CountsValue value )
    {
        if ( value.count > 0 )
        {
            writer.merge( key, value, MERGER );
        }
        else if ( value.count == 0 )
        {
            writer.remove( key );
        }
        else
        {
            throw new IllegalStateException( "Count for " + key + " got negative: " + value.count );
        }
    }
}
