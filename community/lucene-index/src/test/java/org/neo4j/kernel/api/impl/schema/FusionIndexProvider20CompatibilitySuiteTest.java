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
package org.neo4j.kernel.api.impl.schema;

import java.io.File;

import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexProviderCompatibilityTestSuite;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.OperationalMode;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SchemaIndex.NATIVE20;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class FusionIndexProvider20CompatibilitySuiteTest extends IndexProviderCompatibilityTestSuite
{
    @Override
    protected IndexProvider createIndexProvider( PageCache pageCache, FileSystemAbstraction fs, File graphDbDir )
    {
        IndexProvider.Monitor monitor = IndexProvider.Monitor.EMPTY;
        Config config = Config.defaults( stringMap( default_schema_provider.name(), NATIVE20.providerName() ) );
        OperationalMode mode = OperationalMode.single;
        RecoveryCleanupWorkCollector recoveryCleanupWorkCollector = RecoveryCleanupWorkCollector.immediate();
        return NativeLuceneFusionIndexProviderFactory20.create( pageCache, graphDbDir, fs, monitor, config, mode, recoveryCleanupWorkCollector );
    }

    @Override
    public boolean supportsSpatial()
    {
        return true;
    }

    @Override
    public boolean supportsTemporal()
    {
        return true;
    }
}
