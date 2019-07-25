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

import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexProviderCompatibilityTestSuite;
import org.neo4j.kernel.impl.factory.OperationalMode;

import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex.NATIVE30;
import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;

public class FusionIndexProvider30CompatibilitySuiteTest extends IndexProviderCompatibilityTestSuite
{
    @Override
    protected IndexProvider createIndexProvider( PageCache pageCache, FileSystemAbstraction fs, File graphDbDir )
    {
        IndexProvider.Monitor monitor = IndexProvider.Monitor.EMPTY;
        Config config = Config.defaults( default_schema_provider, NATIVE30.providerName() );
        OperationalMode mode = OperationalMode.SINGLE;
        RecoveryCleanupWorkCollector recoveryCleanupWorkCollector = RecoveryCleanupWorkCollector.immediate();
        return NativeLuceneFusionIndexProviderFactory30.create( pageCache, graphDbDir, fs, monitor, config, mode, recoveryCleanupWorkCollector );
    }

    @Override
    public boolean supportsSpatial()
    {
        return true;
    }
}
