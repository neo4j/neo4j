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
package org.neo4j.test;

import static org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils.readEntry;
import static org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils.writeLogEntry;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.TxLog;
import org.neo4j.kernel.impl.transaction.xaframework.DirectMappedLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.kernel.impl.util.DumpLogicalLog.CommandFactory;

public class LogTestUtils
{
    public static final Predicate<Pair<Byte, List<byte[]>>> EVERYTHING_BUT_DONE_RECORDS = new Predicate<Pair<Byte,List<byte[]>>>()
    {
        @Override
        public boolean accept( Pair<Byte, List<byte[]>> item )
        {
            return item.first().byteValue() != TxLog.TX_DONE;
        }
    };
    
    public static void filterTxLog( String storeDir, Predicate<Pair<Byte, List<byte[]>>> filter ) throws Exception
    {
        File file = oneOrTwo( new File( storeDir, "tm_tx_log" ) );
        File tempFile = new File( file.getAbsolutePath() + ".tmp" );
        FileChannel in = new RandomAccessFile( file, "r" ).getChannel();
        FileChannel out = new RandomAccessFile( tempFile, "rw" ).getChannel();
        LogBuffer outBuffer = new DirectMappedLogBuffer( out );
        ByteBuffer buffer = ByteBuffer.allocate( 1024*1024 );
        try
        {
            in.read( buffer );
            buffer.flip();
            while ( buffer.hasRemaining() )
            {
                byte type = buffer.get();
                List<byte[]> xids = null;
                if ( type == TxLog.TX_START ) xids = readXids( buffer, 1 );
                else if ( type == TxLog.BRANCH_ADD ) xids = readXids( buffer, 2 );
                else if ( type == TxLog.MARK_COMMIT ) xids = readXids( buffer, 1 );
                else if ( type == TxLog.TX_DONE ) xids = readXids( buffer, 1 );
                else throw new IllegalArgumentException( "Unknown type " + type );
                
                if ( filter.accept( Pair.of( type, xids ) ) )
                {
                    outBuffer.put( type );
                    writeXids( xids, outBuffer );
                }
            }
        }
        finally
        {
            safeClose( in );
            outBuffer.force();
            safeClose( out );
        }
        
        file.delete();
        tempFile.renameTo( file );
    }
    
    public static void filterNeostoreLogicalLog( String storeDir, Predicate<LogEntry> filter ) throws IOException
    {
        File file = oneOrTwo( new File( storeDir, NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME ) );
        File tempFile = new File( file.getAbsolutePath() + ".tmp" );
        FileChannel in = new RandomAccessFile( file, "r" ).getChannel();
        FileChannel out = new RandomAccessFile( tempFile, "rw" ).getChannel();
        LogBuffer outBuffer = new DirectMappedLogBuffer( out );
        ByteBuffer buffer = ByteBuffer.allocate( 1024*1024 );
        transferLogicalLogHeader( in, outBuffer, buffer );
        CommandFactory cf = new CommandFactory();
        try
        {
            LogEntry entry = null;
            while ( (entry = readEntry( buffer, in, cf ) ) != null )
            {
                if ( !filter.accept( entry ) ) continue;
                writeLogEntry( entry, outBuffer );
            }
        }
        finally
        {
            safeClose( in );
            outBuffer.force();
            safeClose( out );
        }

        file.delete();
        tempFile.renameTo( file );
    }

    private static void transferLogicalLogHeader( FileChannel in, LogBuffer outBuffer,
            ByteBuffer buffer ) throws IOException
    {
        long[] header = LogIoUtils.readLogHeader( buffer, in, true );
        LogIoUtils.writeLogHeader( buffer, header[0], header[1] );
        byte[] headerBytes = new byte[buffer.limit()];
        buffer.get( headerBytes );
        outBuffer.put( headerBytes );
    }

    private static void safeClose( FileChannel channel )
    {
        try
        {
            if ( channel != null ) channel.close();
        }
        catch ( IOException e )
        {   // OK
        }
    }

    private static void writeXids( List<byte[]> xids, LogBuffer outBuffer ) throws IOException
    {
        for ( byte[] xid : xids ) outBuffer.put( (byte)xid.length );
        for ( byte[] xid : xids ) outBuffer.put( xid );
    }

    private static List<byte[]> readXids( ByteBuffer buffer, int count )
    {
        byte[] counts = new byte[count];
        for ( int i = 0; i < count; i++ ) counts[i] = buffer.get();
        List<byte[]> xids = new ArrayList<byte[]>();
        for ( int i = 0; i < count; i++ ) xids.add( readXid( buffer, counts[i] ) );
        return xids;
    }

    private static byte[] readXid( ByteBuffer buffer, byte length )
    {
        byte[] bytes = new byte[length];
        buffer.get( bytes );
        return bytes;
    }

    private static File oneOrTwo( File file )
    {
        File one = new File( file.getAbsolutePath() + ".1" );
        if ( one.exists() ) return one;
        File two = new File( file.getAbsolutePath() + ".2" );
        if ( two.exists() ) return two;
        throw new IllegalStateException( "Couldn't find any active tm log" );
    }
}
