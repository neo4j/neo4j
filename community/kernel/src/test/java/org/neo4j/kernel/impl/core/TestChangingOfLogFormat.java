/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.Pair;

import static org.junit.Assert.*;
import static org.neo4j.kernel.impl.AbstractNeo4jTestCase.deleteFileOrDirectory;
import static org.neo4j.kernel.impl.nioneo.store.TestXa.*;

public class TestChangingOfLogFormat
{
    @Test
    public void inabilityToStartFromOldFormatFromNonCleanShutdown() throws Exception
    {
        String storeDir = "target/var/oldlog";
        deleteFileOrDirectory( storeDir ); 
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        Transaction tx = db.beginTx();
        db.createNode();
        tx.success();
        tx.finish();
        
        Pair<Pair<File, File>, Pair<File, File>> copy = copyLogicalLog( storeDir );
        decrementLogFormat( copy.other().other() );
        db.shutdown();
        renameCopiedLogicalLog( storeDir );
        
        try
        {
            db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
            fail( "Shouldn't be able to do recovery (and upgrade log format version) on non-clean shutdown" );
        }
        catch ( Exception e )
        {   // Good
            e.printStackTrace();
        }
    }
    
    private void decrementLogFormat( File file ) throws IOException
    {
        // Gotten from LogIoUtils class
        RandomAccessFile raFile = new RandomAccessFile( file, "rw" );
        FileChannel channel = raFile.getChannel();
        ByteBuffer buffer = ByteBuffer.wrap( new byte[8] );
        channel.read( buffer );
        buffer.flip();
        long version = buffer.getLong();
        long logFormatVersion = (version >>> 56);
        version = version & 0x00FFFFFFFFFFFFFFL;
        long oldVersion = version | ( ((long) logFormatVersion-1) << 56 );
        channel.position( 0 );
        buffer.clear();
        buffer.putLong( oldVersion );
        buffer.flip();
        channel.write( buffer );
        raFile.close();
    }
}
