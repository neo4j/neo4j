/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.recovery.StoreRecoverer;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertTrue;

import static org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory.createPageCache;

public class TestStoreAccess
{
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final File storeDir = new File( "dir" ).getAbsoluteFile();

    @Test
    public void openingThroughStoreAccessShouldNotTriggerRecovery() throws Exception
    {
        EphemeralFileSystemAbstraction snapshot = produceUncleanStore();
        assertTrue( "Store should be unclean", isUnclean( snapshot ) );
        File messages = new File( storeDir, "messages.log" );
        snapshot.deleteFile( messages );
        LifeSupport life = new LifeSupport();
        life.start();;

        PageCache pageCache = createPageCache( snapshot, getClass().getName(), life );
        try
        {
            new StoreAccess( snapshot, pageCache, storeDir.getPath() ).close();
            assertTrue( "Store should be unclean", isUnclean( snapshot ) );
        }
        finally
        {
            pageCache.close();
            life.shutdown();
        }
    }

    private EphemeralFileSystemAbstraction produceUncleanStore()
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory().setFileSystem( fs.get() )
                .newImpermanentDatabase( storeDir.getPath() );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
        EphemeralFileSystemAbstraction snapshot = fs.get().snapshot();
        db.shutdown();
        return snapshot;
    }

    private boolean isUnclean( FileSystemAbstraction fileSystem ) throws IOException
    {
        return new StoreRecoverer( fileSystem ).recoveryNeededAt( storeDir );
    }
}
