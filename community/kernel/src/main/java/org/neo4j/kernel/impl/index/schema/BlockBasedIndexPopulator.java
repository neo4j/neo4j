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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyAccessor;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.util.FeatureToggles;

import static org.neo4j.kernel.impl.index.schema.NativeIndexUpdater.initializeKeyFromUpdate;

public class BlockBasedIndexPopulator<KEY extends NativeIndexKey<KEY>,VALUE extends NativeIndexValue> implements IndexPopulator
{
    private static final String BLOCK_SIZE = FeatureToggles.getString( BlockBasedIndexPopulator.class, "blockSize", "1M" );

    private final Layout<KEY,VALUE> layout;
    private final int blockSize;
    private BlockStorage<KEY,VALUE> blockStorage;

    BlockBasedIndexPopulator( Layout<KEY,VALUE> layout )
    {
        this.layout = layout;

        long parse = ByteUnit.parse( BLOCK_SIZE );
        if ( (parse & ~0xFFFF_FFFFL) != 0 )
        {
            throw new IllegalArgumentException( "Block size need to fit in int. Was " + parse );
        }
        blockSize = (int) parse;
    }

    @Override
    public void create()
    {
        // Clean directory
    }

    @Override
    public void drop()
    {
        // Make responsive
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates )
    {
        try
        {
            for ( IndexEntryUpdate<?> update : updates )
            {
                KEY key = layout.newKey();
                VALUE value = layout.newValue();
                initializeKeyFromUpdate( key, update.getEntityId(), update.values() );
                value.from( update.values() );

                blockStorage.add( key, value );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    void finishUp() throws IOException
    {
        blockStorage.doneAdding();
        blockStorage.merge();
//         for ( ... ) // Multi thread
//        blockStorage.merge();
//        Iterator<KEY,VALUE> iter = blockStorage.read();
//        buildTree();
//        ...
    }

    @Override
    public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor ) throws IndexEntryConflictException
    {
        // On building tree
    }

    @Override
    public IndexUpdater newPopulatingUpdater( NodePropertyAccessor accessor )
    {
        // Updater that buffers all updates and apply them after tree is built
        return null;
    }

    @Override
    public void close( boolean populationCompletedSuccessfully )
    {
        // Make responsive
    }

    @Override
    public void markAsFailed( String failure )
    {

    }

    @Override
    public void includeSample( IndexEntryUpdate<?> update )
    {
        // leave for now, can either sample when we build tree in the end or when updates come in
    }

    @Override
    public IndexSample sampleResult()
    {
        // leave for now, look at what NativeIndexPopulator does
        return null;
    }
}
