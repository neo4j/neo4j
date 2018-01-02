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
package org.neo4j.legacy.consistency.store;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.legacy.consistency.ConsistencyCheckService;
import org.neo4j.legacy.consistency.ConsistencyCheckSettings;
import org.neo4j.legacy.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class StoreAssertions
{
    private StoreAssertions()
    {
    }

    public static void assertConsistentStore( File dir ) throws ConsistencyCheckIncompleteException, IOException
    {
        final Config configuration =
                new Config(
                        stringMap( GraphDatabaseSettings.pagecache_memory.name(), "8m" ),
                        GraphDatabaseSettings.class,
                        ConsistencyCheckSettings.class
                );

        final ConsistencyCheckService.Result result =
                new ConsistencyCheckService().runFullConsistencyCheck(
                        dir,
                        configuration,
                        ProgressMonitorFactory.NONE,
                        NullLogProvider.getInstance(),
                        new DefaultFileSystemAbstraction()
                );

        assertTrue( result.isSuccessful() );
    }
}
