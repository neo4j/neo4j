/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.monitoring.Monitors;

import static org.neo4j.kernel.api.impl.index.storage.DirectoryFactory.directoryFactory;

public class IndexProviderFactoryUtil
{
    public static boolean isReadOnly( Config config, boolean isSingleInstance )
    {
        return config.get( GraphDatabaseSettings.read_only ) && isSingleInstance;
    }

    public static LuceneIndexProvider luceneProvider( FileSystemAbstraction fs, IndexDirectoryStructure.Factory directoryStructure,
            Monitors monitors, String monitorTag, Config config, boolean isSingleInstance )
    {
        boolean ephemeral = config.get( GraphDatabaseInternalSettings.ephemeral_lucene );
        DirectoryFactory directoryFactory = directoryFactory( ephemeral );
        return new LuceneIndexProvider( fs, directoryFactory, directoryStructure, monitors, monitorTag, config, isSingleInstance );
    }
}
