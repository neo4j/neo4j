/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.schema;

import java.io.File;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexProviderCompatibilityTestSuite;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.factory.OperationalMode;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class LuceneSchemaIndexProviderCompatibilitySuiteTest extends IndexProviderCompatibilityTestSuite
{
    @Override
    protected LuceneSchemaIndexProvider createIndexProvider( PageCache pageCache, FileSystemAbstraction fs, File graphDbDir )
    {
        DirectoryFactory.InMemoryDirectoryFactory directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        SchemaIndexProvider.Monitor monitor = SchemaIndexProvider.Monitor.EMPTY;
        Config config = Config.defaults( stringMap( GraphDatabaseSettings.enable_native_schema_index.name(), Settings.FALSE ) );
        OperationalMode mode = OperationalMode.single;
        return LuceneSchemaIndexProviderFactory.create( fs, graphDbDir, monitor, config, mode );
    }
}
