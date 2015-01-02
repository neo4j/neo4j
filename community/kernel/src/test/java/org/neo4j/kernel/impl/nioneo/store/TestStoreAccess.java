/**
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class TestStoreAccess
{
    @Test
    public void openingThroughStoreAccessShouldNotTriggerRecovery() throws Exception
    {
        EphemeralFileSystemAbstraction snapshot = produceUncleanStore();
        assertTrue( "Store should be unclean", isUnclean( snapshot ) );
        File messages = new File( storeDir, "messages.log" );
        snapshot.deleteFile( messages );
        
        new StoreAccess( snapshot, storeDir.getPath(), stringMap() ).close();
        assertTrue( "Store should be unclean", isUnclean( snapshot ) );
    }
    
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final File storeDir = new File( "dir" );
    
    private EphemeralFileSystemAbstraction produceUncleanStore()
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory().setFileSystem( fs.get() )
                .newImpermanentDatabase( storeDir.getPath() );
        EphemeralFileSystemAbstraction snapshot = fs.get().snapshot();
        db.shutdown();
        return snapshot;
    }

    private boolean isUnclean( FileSystemAbstraction fileSystem ) throws IOException
    {
        char chr = activeLog( fileSystem, storeDir );
        return chr == '1' || chr == '2';
    }

    private char activeLog( FileSystemAbstraction fileSystem, File directory ) throws IOException
    {
        StoreChannel file = fileSystem.open( new File( directory, "nioneo_logical.log.active" ), "r" );
        try
        {
            ByteBuffer buffer = ByteBuffer.wrap( new byte[2] );
            file.read( buffer );
            buffer.flip();
            return buffer.getChar();
        }
        finally
        {
            file.close();
        }
    }
}
