/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;

import java.io.IOException;
import java.util.Arrays;

import org.neo4j.index.internal.gbptree.ValueMerger;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.storageengine.api.EntityTokenUpdate;

import static java.lang.Math.toIntExact;
import static org.neo4j.kernel.impl.index.schema.NativeTokenScanWriter.offsetOf;
import static org.neo4j.kernel.impl.index.schema.NativeTokenScanWriter.rangeOf;
import static org.neo4j.util.Preconditions.checkArgument;

/**
 * Writer that takes a more batching approach to adding data to the token scan store. It is optimized for writing large amounts of entity-id-sequential updates,
 * especially when there are lots of tokens involved. It works by having an array of ranges, slot index is tokenId. Updates that comes in
 * will find the slot by tokenId and add the correct bit to the current range, or if the bit is in another range, merge the current one first.
 * It cannot handle updates to entities that are already in ths token index, such operations will fail before trying to make those changes.
 */
class BulkAppendNativeTokenScanWriter implements TokenScanWriter
{
    private final Writer<TokenScanKey,TokenScanValue> writer;
    private final ValueMerger<TokenScanKey,TokenScanValue> merger;
    private final MutableIntObjectMap<Pair<TokenScanKey,TokenScanValue>> ranges = IntObjectMaps.mutable.empty();

    BulkAppendNativeTokenScanWriter( Writer<TokenScanKey,TokenScanValue> writer )
    {
        this.writer = writer;
        this.merger = new AddMerger( NativeTokenScanWriter.EMPTY );
    }

    @Override
    public void write( EntityTokenUpdate update )
    {
        checkArgument( update.getTokensBefore().length == 0, "Was expecting no tokens before, was %s", Arrays.toString( update.getTokensBefore() ) );
        long idRange = rangeOf( update.getEntityId() );
        int previousTokenId = -1;
        for ( long tokenId : update.getTokensAfter() )
        {
            int intTokenId = toIntExact( tokenId );
            checkArgument( intTokenId > previousTokenId, "Detected unsorted tokens in %s", update );
            previousTokenId = intTokenId;
            Pair<TokenScanKey,TokenScanValue> range =
                    ranges.getIfAbsentPutWithKey( intTokenId, id -> Pair.of( new TokenScanKey( id, idRange ), new TokenScanValue() ) );
            if ( range.getKey().idRange != idRange )
            {
                if ( range.getKey().idRange != -1 )
                {
                    writer.merge( range.getKey(), range.getValue(), merger );
                }
                range.getKey().idRange = idRange;
                range.getValue().clear();
            }
            range.getValue().set( offsetOf( update.getEntityId() ) );
        }
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            for ( Pair<TokenScanKey,TokenScanValue> range : ranges )
            {
                if ( range != null )
                {
                    writer.merge( range.getKey(), range.getValue(), merger );
                }
            }
        }
        finally
        {
            writer.close();
        }
    }
}
