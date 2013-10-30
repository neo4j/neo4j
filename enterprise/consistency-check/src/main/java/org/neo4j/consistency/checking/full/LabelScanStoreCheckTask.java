/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking.full;

import java.io.IOException;

import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.ComparativeRecordChecker;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.synthetic.LabelScanDocument;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.scan.LabelScanStore;
import org.neo4j.kernel.api.scan.NodeLabelRange;
import org.neo4j.kernel.api.scan.NodeRangeReader;
import org.neo4j.kernel.api.scan.NodeRangeScanSupport;
import org.neo4j.kernel.api.scan.ScannableStores;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;

public class LabelScanStoreCheckTask implements StoppableRunnable,
        RecordCheck<LabelScanDocument, ConsistencyReport.LabelScanConsistencyReport>
{
    private final NodeRangeScanSupport scanSupport;
    private final ConsistencyReporter reporter;
    private final ProgressListener progress;

    private volatile boolean continueScanning = true;

    public LabelScanStoreCheckTask( ScannableStores stores, ProgressMonitorFactory.MultiPartBuilder builder,
                                    ConsistencyReporter reporter )
    {
        this.reporter = reporter;
        LabelScanStore labelScanStore = stores.labelScanStore();
        this.scanSupport =
                labelScanStore instanceof NodeRangeScanSupport ? (NodeRangeScanSupport) labelScanStore : null;
        this.progress = buildProgressListener( builder, scanSupport );
    }

    private static ProgressListener buildProgressListener( ProgressMonitorFactory.MultiPartBuilder builder,
                                                           NodeRangeScanSupport scanSupport )
    {
        try
        {
            return builder.progressForPart( "LabelScanStore", scanSupport.getHighRangeId() );
        }
        catch ( IOException e )
        {
            ProgressListener listener = builder.progressForPart( "LabelScanStore", 0 );
            listener.failed( e );
            return null;
        }
    }

    @Override
    public void run()
    {
        if ( progress == null )
        {
            return;
        }

        try
        {
            if ( scanSupport == null )
            {
                return;
            }

            try ( NodeRangeReader reader = scanSupport.newRangeReader() )
            {
                for ( NodeLabelRange range : reader )
                {
                    if ( continueScanning )
                    {
                        LabelScanDocument document = new LabelScanDocument( range.id(), range );
                        reporter.forNodeLabelScan( document, this );
                    }
                    else
                    {
                        return;
                    }
                }
            }
            catch ( IOException e )
            {
                progress.failed( e );
            }
        }
        finally
        {
            progress.done();
        }
    }

    @Override
    public void stopScanning()
    {
        continueScanning = false;
    }

    @Override
    public void check( LabelScanDocument record, CheckerEngine<LabelScanDocument,
            ConsistencyReport.LabelScanConsistencyReport> engine, RecordAccess records )
    {
        NodeLabelRange range = record.getNodeLabelRange();
        for ( long nodeId : range.nodes() )
        {
            engine.comparativeCheck( records.node( nodeId ), new NodeRecordCheck() );
        }
    }

    @Override
    public void checkChange( LabelScanDocument oldRecord, LabelScanDocument newRecord,
                             CheckerEngine<LabelScanDocument,
                                     ConsistencyReport.LabelScanConsistencyReport> engine, DiffRecordAccess records )
    {
        throw new UnsupportedOperationException();
    }

    private class NodeRecordCheck implements
            ComparativeRecordChecker<LabelScanDocument, NodeRecord, ConsistencyReport.LabelScanConsistencyReport>
    {
        @Override
        public void checkReference( LabelScanDocument record, NodeRecord nodeRecord, CheckerEngine<LabelScanDocument,
                ConsistencyReport.LabelScanConsistencyReport> engine, RecordAccess records )
        {
            if ( nodeRecord.inUse() )
            {
                // TODO: more checks coming here
            }
            else
            {
                engine.report().nodeNotInUse( nodeRecord );
            }
        }
    }
}
