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
package org.neo4j.kernel.impl.core;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

import static org.junit.Assert.fail;

public class TestChangingOfLogFormat
{
    @Test
    public void inabilityToStartFromOldFormatFromNonCleanShutdown() throws Exception
    {
        File storeDir = new File( "target/var/oldlog" );
        GraphDatabaseService db = factory.newImpermanentDatabase( storeDir.getPath() );
        File logBaseFileName = ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( Config.class ).get( GraphDatabaseSettings.logical_log );
        Transaction tx = db.beginTx();
        db.createNode();
        tx.success();
        tx.finish();

        Pair<Pair<File, File>, Pair<File, File>> copy = copyLogicalLog( logBaseFileName );
        decrementLogFormat( copy.other().other() );
        db.shutdown();
        renameCopiedLogicalLog( copy );

        try
        {
            db = factory.newImpermanentDatabase( storeDir.getPath() );
            fail( "Shouldn't be able to do recovery (and upgrade log format version) on non-clean shutdown" );
        }
        catch ( Exception e )
        {   // Good
        }
    }

    private void decrementLogFormat( File file ) throws IOException
    {
        // Gotten from LogIoUtils class
        StoreChannel channel = fs.get().open( file, "rw" );
        ByteBuffer buffer = ByteBuffer.wrap( new byte[8] );
        channel.read( buffer );
        buffer.flip();
        long version = buffer.getLong();
        long logFormatVersion = (version >>> 56);
        version = version & 0x00FFFFFFFFFFFFFFL;
        long oldVersion = version | ( (logFormatVersion-1) << 56 );
        channel.position( 0 );
        buffer.clear();
        buffer.putLong( oldVersion );
        buffer.flip();
        channel.write( buffer );
        channel.close();
    }

    public  Pair<Pair<File, File>, Pair<File, File>> copyLogicalLog( File logBaseFileName ) throws IOException
    {
        EphemeralFileSystemAbstraction fileSystem = fs.get();
        File activeLog = new File( logBaseFileName.getPath() + ".active" );
        StoreChannel af = fileSystem.open( activeLog, "r" );
        ByteBuffer buffer = ByteBuffer.allocate( 1024 );
        af.read( buffer );
        buffer.flip();
        File activeLogBackup = new File( logBaseFileName.getPath() + ".bak.active" );
        StoreChannel activeCopy = fileSystem.open( activeLogBackup, "rw" );
        activeCopy.write( buffer );
        activeCopy.close();
        af.close();
        buffer.flip();
        char active = buffer.asCharBuffer().get();
        buffer.clear();
        File currentLog = new File( logBaseFileName.getPath() + "." + active );
        StoreChannel source = fileSystem.open( currentLog, "r" );
        File currentLogBackup = new File( logBaseFileName.getPath() + ".bak." + active );
        StoreChannel dest = fileSystem.open( currentLogBackup, "rw" );
        int read;
        do
        {
            read = source.read( buffer );
            buffer.flip();
            dest.write( buffer );
            buffer.clear();
        }
        while ( read == 1024 );
        source.close();
        dest.close();
        return Pair.of( Pair.of( activeLog, activeLogBackup ), Pair.of( currentLog, currentLogBackup ) );
    }

    public void renameCopiedLogicalLog( Pair<Pair<File, File>, Pair<File, File>> files ) throws IOException
    {
        EphemeralFileSystemAbstraction fileSystem = fs.get();
        fileSystem.deleteFile( files.first().first() );
        fileSystem.renameFile( files.first().other(), files.first().first() );

        fileSystem.deleteFile( files.other().first() );
        fileSystem.renameFile( files.other().other(), files.other().first() );
    }

    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private TestGraphDatabaseFactory factory;

    @Before
    public void before() throws Exception
    {
        factory = new TestGraphDatabaseFactory().setFileSystem( fs.get() );
    }

    @After
    public void after() throws Exception
    {
    }
}
