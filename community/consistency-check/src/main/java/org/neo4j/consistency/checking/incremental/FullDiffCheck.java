/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.consistency.checking.incremental;

import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.FullCheck;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.store.DiffStore;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.index.lucene.LuceneLabelScanStoreBuilder;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.direct.DirectStoreAccess;
import org.neo4j.kernel.api.impl.index.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class FullDiffCheck extends DiffCheck
{
    public FullDiffCheck( StringLogger logger )
    {
        super( logger );
    }

    @Override
    public ConsistencySummaryStatistics execute( DiffStore diffs ) throws ConsistencyCheckIncompleteException
    {
        Config tuningConfiguration = new Config( stringMap(), GraphDatabaseSettings.class,
                ConsistencyCheckSettings.class );

        String storeDir = tuningConfiguration.get( GraphDatabaseSettings.store_dir ).getAbsolutePath();
        DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        LabelScanStore labelScanStore =
            new LuceneLabelScanStoreBuilder( storeDir, diffs.getRawNeoStore(), fileSystem, logger ).build();

        SchemaIndexProvider indexes = new LuceneSchemaIndexProvider( DirectoryFactory.PERSISTENT, tuningConfiguration );
        DirectStoreAccess stores = new DirectStoreAccess( diffs, labelScanStore, indexes );
        return new FullCheck( tuningConfiguration, ProgressMonitorFactory.NONE ).execute( stores, logger );
    }
}
