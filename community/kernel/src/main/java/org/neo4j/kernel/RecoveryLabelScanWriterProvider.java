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
package org.neo4j.kernel;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.helpers.Provider;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.SORT_BY_NODE_ID;

/**
 * Provides {@link LabelScanWriter} that takes advantage of the single-threaded context of recovery
 * to cache writes and apply in bigger batches, where each batch holds data from many transactions.
 */
public class RecoveryLabelScanWriterProvider implements Provider<LabelScanWriter>, Closeable
{
    private int callCount;
    private final LabelScanStore labelScanStore;
    private final RecoveryLabelScanWriter recoveryWriter = new RecoveryLabelScanWriter();
    private final int batchSize;

    public RecoveryLabelScanWriterProvider( LabelScanStore labelScanStore, int batchSize )
    {
        this.labelScanStore = labelScanStore;
        this.batchSize = batchSize;
    }

    /**
     * Called once for every transactions that have any label updates.
     */
    @Override
    public LabelScanWriter instance()
    {
        return recoveryWriter;
    }

    /**
     * Called AFTER all transactions have been recovered, before forcing everything.
     */
    @Override
    public void close() throws IOException
    {
        recoveryWriter.writePendingUpdates();
    }

    private class RecoveryLabelScanWriter implements LabelScanWriter
    {
        private final List<NodeLabelUpdate> updates = new ArrayList<>();

        @Override
        public void write( NodeLabelUpdate update ) throws IOException, IndexCapacityExceededException
        {
            updates.add( update );
        }

        /**
         * Called from the transaction applier code. We won't close the actual writer every time
         * we get this call, only every {@link RecoveryLabelScanWriterProvider#BATCH_SIZE} time,
         * at which point {@link #writePendingUpdates()} is called.
         */
        @Override
        public void close() throws IOException
        {
            if ( ++callCount % batchSize == 0 )
            {
                writePendingUpdates();
            }
        }

        private void writePendingUpdates() throws IOException
        {
            if ( !updates.isEmpty() )
            {
                Collections.sort( updates, SORT_BY_NODE_ID );
                try ( LabelScanWriter writer = labelScanStore.newWriter() )
                {
                    for ( NodeLabelUpdate update : updates )
                    {
                        writer.write( update );
                    }
                }
                catch ( IndexCapacityExceededException e )
                {
                    throw new IOException( e );
                }
                updates.clear();
            }
        }
    }
}
