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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;

/**
 * Writes {@link NodeRecord} to store. Contains a {@link #accept(NodeRecord) predicate} to override
 * for forcing some records to be set to {@link NodeRecord#setInUse(boolean) not in use}.
 * Also deletes such unused records from {@link LabelScanStore}.
 */
public class UpdateNodeRecordsStep extends UpdateRecordsStep<NodeRecord>
{
    private final PrimitiveLongIterator ids;
    private long current;
    private boolean end;
    private final LabelScanWriter labelScanWriter;

    public UpdateNodeRecordsStep( StageControl control, Configuration config, RecordStore<NodeRecord> store,
            Collector collector, LabelScanStore labelScanStore )
    {
        super( control, config, store );
        this.ids = collector.leftOverDuplicateNodesIds();
        goToNextId();
        this.labelScanWriter = end ? LabelScanWriter.EMPTY : labelScanStore.newWriter();
    }

    private void goToNextId()
    {
        this.end = !ids.hasNext();
        if ( !end )
        {
            this.current = ids.next();
        }
    }

    @Override
    protected boolean accept( NodeRecord node )
    {
        if ( !end && current == node.getId() )
        {   // Found an id to exclude, exclude it and go to the next (they are sorted)
            goToNextId();
            return false;
        }
        return true;
    }

    @Override
    protected void update( NodeRecord node ) throws Throwable
    {
        super.update( node );
        if ( !node.inUse() )
        {
            // Only the "labelsAfter" is considered
            labelScanWriter.write( NodeLabelUpdate.labelChanges( current, EMPTY_LONG_ARRAY, EMPTY_LONG_ARRAY ) );
        }
    }

    @Override
    public void close() throws Exception
    {
        super.close();
        labelScanWriter.close();
    }
}
