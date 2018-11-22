/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.store;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.configuration.Config.defaults;
import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectoryExtension.class} )
class TestStoreAccess
{
    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension();
    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private final Monitors monitors = new Monitors();

    @Test
    void openingThroughStoreAccessShouldNotTriggerRecovery() throws Throwable
    {
        try ( EphemeralFileSystemAbstraction snapshot = produceUncleanStore() )
        {
            assertTrue( isUnclean( snapshot ), "Store should be unclean" );

            PageCache pageCache = pageCacheExtension.getPageCache( snapshot );
            new StoreAccess( snapshot, pageCache, testDirectory.databaseLayout(), Config.defaults() ).initialize().close();
            assertTrue( isUnclean( snapshot ), "Store should be unclean" );
        }
    }

    private EphemeralFileSystemAbstraction produceUncleanStore()
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory().setFileSystem( fs )
                .newImpermanentDatabase( testDirectory.databaseDir() );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
        EphemeralFileSystemAbstraction snapshot = fs.snapshot();
        db.shutdown();
        return snapshot;
    }

    private boolean isUnclean( FileSystemAbstraction fileSystem ) throws Exception
    {
        return isRecoveryRequired( fileSystem, testDirectory.databaseLayout(), defaults() );
    }
}
