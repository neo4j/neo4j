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

import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.synthetic.LabelScanDocument;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.direct.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.direct.DirectStoreAccess;
import org.neo4j.kernel.api.direct.NodeLabelRange;

public class LabelScanStoreCheckTask implements StoppableRunnable
{
    private final AllEntriesLabelScanReader reader;
    private final ConsistencyReport.Reporter reporter;
    private final ProgressListener progress;
    private final LabelScanCheck check;

    private volatile boolean continueScanning = true;

    public LabelScanStoreCheckTask( DirectStoreAccess stores, ProgressMonitorFactory.MultiPartBuilder builder,
                                    ConsistencyReport.Reporter reporter, LabelScanCheck check )
    {
        this.reporter = reporter;
        this.reader = stores.labelScanStore().newAllEntriesReader();
        this.progress = buildProgressListener( builder, reader );
        this.check = check;
    }

    private static ProgressListener buildProgressListener( ProgressMonitorFactory.MultiPartBuilder builder,
                                                           AllEntriesLabelScanReader reader )
    {
        try
        {
            return builder.progressForPart( "LabelScanStore", reader.getHighRangeId() );
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
            for ( NodeLabelRange range : reader )
            {
                if ( !continueScanning )
                {
                    return;
                }
                LabelScanDocument document = new LabelScanDocument( range );
                reporter.forNodeLabelScan( document, check );
                progress.set(range.id());
            }
        }
        finally
        {
            try
            {
                reader.close();
            }
            catch ( IOException e )
            {
                progress.failed( e );
            }
            progress.done();
        }
    }

    @Override
    public void stopScanning()
    {
        continueScanning = false;
    }
}
