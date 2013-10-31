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
import org.neo4j.consistency.checking.LabelChainWalker;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.consistency.store.synthetic.LabelScanDocument;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.direct.DirectStoreAccess;
import org.neo4j.kernel.api.direct.NodeLabelRange;
import org.neo4j.kernel.api.direct.NodeRangeReader;
import org.neo4j.kernel.api.direct.NodeRangeScanSupport;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.labels.DynamicNodeLabels;
import org.neo4j.kernel.impl.nioneo.store.labels.NodeLabels;
import org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField;

import static java.util.Arrays.binarySearch;
import static java.util.Arrays.sort;

public class LabelScanStoreCheckTask implements StoppableRunnable,
        RecordCheck<LabelScanDocument, ConsistencyReport.LabelScanConsistencyReport>
{
    private final NodeRangeScanSupport scanSupport;
    private final ConsistencyReporter reporter;
    private final ProgressListener progress;

    private volatile boolean continueScanning = true;

    public LabelScanStoreCheckTask( DirectStoreAccess stores, ProgressMonitorFactory.MultiPartBuilder builder,
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
                        LabelScanDocument document = new LabelScanDocument( range );
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

    static class NodeRecordCheck implements
            ComparativeRecordChecker<LabelScanDocument, NodeRecord, ConsistencyReport.LabelScanConsistencyReport>
    {
        @Override
        public void checkReference( LabelScanDocument record, NodeRecord nodeRecord, CheckerEngine<LabelScanDocument,
                ConsistencyReport.LabelScanConsistencyReport> engine, RecordAccess records )
        {
            if ( nodeRecord.inUse() )
            {
                long[] expectedLabels = record.getNodeLabelRange().labels( nodeRecord.getId() );
                // assert that node has expected labels

                NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( nodeRecord );
                if ( nodeLabels instanceof DynamicNodeLabels )
                {
                    DynamicNodeLabels dynamicNodeLabels = (DynamicNodeLabels) nodeLabels;
                    long firstRecordId = dynamicNodeLabels.getFirstDynamicRecordId();
                    RecordReference<DynamicRecord> firstRecordReference = records.nodeLabels( firstRecordId );
                    engine.comparativeCheck( firstRecordReference,
                            new LabelChainWalker<LabelScanDocument, ConsistencyReport.LabelScanConsistencyReport>
                                    (new ExpectedNodeLabelsChecker( nodeRecord, expectedLabels )) );
                    nodeRecord.getDynamicLabelRecords(); // I think this is empty in production
                }
                else
                {
                    long[] actualLabels = nodeLabels.get( null );
                    ConsistencyReport.LabelScanConsistencyReport report = engine.report();
                    validateLabelIds( nodeRecord, expectedLabels, actualLabels, report );
                }
            }
            else
            {
                engine.report().nodeNotInUse( nodeRecord );
            }
        }

        private void validateLabelIds( NodeRecord nodeRecord, long[] expectedLabels, long[] actualLabels, ConsistencyReport.LabelScanConsistencyReport report )
        {
            sort(actualLabels);
            for ( long expectedLabel : expectedLabels )
            {
                int labelIndex = binarySearch( actualLabels, expectedLabel );
                if (labelIndex < 0)
                {
                    report.nodeDoesNotHaveExpectedLabel( nodeRecord, expectedLabel );
                }
            }
        }

        private class ExpectedNodeLabelsChecker implements
                LabelChainWalker.Validator<LabelScanDocument, ConsistencyReport.LabelScanConsistencyReport>
        {
            private final NodeRecord nodeRecord;
            private final long[] expectedLabels;

            public ExpectedNodeLabelsChecker( NodeRecord nodeRecord, long[] expectedLabels )
            {
                this.nodeRecord = nodeRecord;
                this.expectedLabels = expectedLabels;
            }

            @Override
            public void onRecordNotInUse( DynamicRecord dynamicRecord, CheckerEngine<LabelScanDocument,
                    ConsistencyReport.LabelScanConsistencyReport> engine )
            {
                // checked elsewhere
            }

            @Override
            public void onRecordChainCycle( DynamicRecord record, CheckerEngine<LabelScanDocument,
                    ConsistencyReport.LabelScanConsistencyReport> engine )
            {
                // checked elsewhere
            }

            @Override
            public void onWellFormedChain( long[] labelIds, CheckerEngine<LabelScanDocument,
                    ConsistencyReport.LabelScanConsistencyReport> engine, RecordAccess records )
            {
                validateLabelIds( nodeRecord, expectedLabels, labelIds, engine.report() );
            }
        }
    }
}
