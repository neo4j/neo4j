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

import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.synthetic.IndexEntry;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.index.AllEntriesIndexReader;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public class IndexCheckTask implements StoppableRunnable
{
    private final IndexAccessor indexAccessor;
    private final AllEntriesIndexReader indexReader;
    private final ProgressListener progress;
    private final ConsistencyReport.Reporter reporter;
    private final RecordCheck<IndexEntry, ConsistencyReport.IndexConsistencyReport> indexCheck;

    private volatile boolean continueScanning = true;

    public IndexCheckTask( IndexRule indexRule, SchemaIndexProvider indexProvider, ProgressMonitorFactory.MultiPartBuilder builder,
                           ConsistencyReport.Reporter reporter, RecordCheck<IndexEntry,
            ConsistencyReport.IndexConsistencyReport> indexCheck )
    {
        this.reporter = reporter;
        this.indexCheck = indexCheck;
        try
        {
            IndexConfiguration indexConfiguration = new IndexConfiguration( indexRule.isConstraintIndex() );
            indexAccessor = indexProvider.getOnlineAccessor( indexRule.getId(), indexConfiguration );
            indexReader = indexAccessor.newAllEntriesReader();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        this.progress = builder.progressForPart( IndexCheckTask.class.getSimpleName() + indexRule.getId(), indexReader.approximateSize() );
    }

    @Override
    public void run()
    {
        try
        {
            int entryCount = 0;
            for ( Long nodeId : indexReader )
            {
                if ( !continueScanning )
                {
                    return;
                }
                reporter.forIndexEntry( new IndexEntry( nodeId ), indexCheck );
                progress.set( entryCount++ );
            }
        }
        finally
        {
            try
            {
                indexReader.close();
                indexAccessor.close();
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
