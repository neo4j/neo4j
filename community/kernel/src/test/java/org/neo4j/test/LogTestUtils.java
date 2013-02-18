/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils.readEntry;
import static org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils.writeLogEntry;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.Xid;

import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.TxLog;
import org.neo4j.kernel.impl.transaction.XidImpl;
import org.neo4j.kernel.impl.transaction.xaframework.DirectMappedLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.kernel.impl.util.DumpLogicalLog.CommandFactory;

/**
 * Utility for reading and filtering logical logs as well as tx logs.
 * 
 * @author Mattias Persson
 */
public class LogTestUtils
{
    public static interface LogHook<RECORD> extends Predicate<RECORD>
    {
        void file( File file );

        void done( File file );
    }
    
    public static abstract class LogHookAdapter<RECORD> implements LogHook<RECORD>
    {
        @Override
        public void file( File file )
        {
        }

        @Override
        public void done( File file )
        {
        }
    }

    public static final LogHook<Pair<Byte, List<byte[]>>> EVERYTHING_BUT_DONE_RECORDS = new LogHookAdapter<Pair<Byte,List<byte[]>>>()
    {
        @Override
        public boolean accept( Pair<Byte, List<byte[]>> item )
        {
            return item.first().byteValue() != TxLog.TX_DONE;
        }
    };
    
    public static final LogHook<Pair<Byte, List<byte[]>>> NO_FILTER = new LogHookAdapter<Pair<Byte,List<byte[]>>>()
    {
        @Override
        public boolean accept( Pair<Byte, List<byte[]>> item )
        {
            return true;
        }
    };
    
    public static final LogHook<Pair<Byte, List<byte[]>>> PRINT_DANGLING = new LogHook<Pair<Byte,List<byte[]>>>()
    {
        private final Map<ByteArray,List<Xid>> xids = new HashMap<ByteArray,List<Xid>>();
        
        @Override
        public boolean accept( Pair<Byte, List<byte[]>> item )
        {
            if ( item.first().byteValue() == TxLog.BRANCH_ADD )
            {
                ByteArray key = new ByteArray( item.other().get( 0 ) );
                List<Xid> list = xids.get( key );
                if ( list == null )
                {
                    list = new ArrayList<Xid>();
                    xids.put( key, list );
                }
                Xid xid = new XidImpl( item.other().get( 0 ), item.other().get( 1 ) );
                list.add( xid );
            }
            else if ( item.first().byteValue() == TxLog.TX_DONE )
            {
                List<Xid> removed = xids.remove( new ByteArray( item.other().get( 0 ) ) );
                if ( removed == null )
                    throw new IllegalArgumentException( "Not found" );
            }
            return true;
        }

        @Override
        public void file( File file )
        {
            xids.clear();
            System.out.println( "=== " + file + " ===" );
        }

        @Override
        public void done( File file )
        {
            for ( List<Xid> xid : xids.values() )
                System.out.println( "dangling " + xid );
        }
    };
    
    public static final LogHook<Pair<Byte, List<byte[]>>> DUMP = new LogHook<Pair<Byte,List<byte[]>>>()
    {
        private int recordCount = 0;
        
        @Override
        public boolean accept( Pair<Byte, List<byte[]>> item )
        {
            System.out.println( stringify( item.first() ) + ": " + stringify( item.other() ) );
            recordCount++;
            return true;
        }
        
        private String stringify( List<byte[]> list )
        {
            if ( list.size() == 2 )
                return new XidImpl( list.get( 0 ), list.get( 1 ) ).toString();
            else if ( list.size() == 1 )
                return stripFromBranch( new XidImpl( list.get( 0 ), new byte[0] ).toString() );
            throw new RuntimeException( list.toString() );
        }

        private String stripFromBranch( String xidToString )
        {
            int index = xidToString.lastIndexOf( ", BranchId" );
            return xidToString.substring( 0, index );
        }

        private String stringify( Byte recordType )
        {
            switch ( recordType.byteValue() )
            {
            case TxLog.TX_START: return "TX_START";
            case TxLog.BRANCH_ADD: return "BRANCH_ADD";
            case TxLog.MARK_COMMIT: return "MARK_COMMIT";
            case TxLog.TX_DONE: return "TX_DONE";
            default: return "Unknown " + recordType;
            }
        }

        @Override
        public void file( File file )
        {
            System.out.println( "=== File:" + file + " ===" );
            recordCount = 0;
        }
        
        @Override
        public void done( File file )
        {
            System.out.println( "===> Read " + recordCount + " records from " + file );
        }
    };
    
    private static class ByteArray
    {
        private final byte[] bytes;

        public ByteArray( byte[] bytes )
        {
            this.bytes = bytes;
        }
        
        @Override
        public boolean equals( Object obj )
        {
            return Arrays.equals( bytes, ((ByteArray)obj).bytes );
        }
        
        @Override
        public int hashCode()
        {
            return Arrays.hashCode( bytes );
        }
    }
    
    public static void filterTxLog( FileSystemAbstraction fileSystem, String storeDir,
            LogHook<Pair<Byte, List<byte[]>>> filter ) throws IOException
    {
        filterTxLog( fileSystem, storeDir, filter, 0 );
    }
    
    public static void filterTxLog( FileSystemAbstraction fileSystem, String storeDir,
            LogHook<Pair<Byte, List<byte[]>>> filter, long startPosition ) throws IOException
    {
        for ( File file : oneOrTwo( new File( storeDir, "tm_tx_log" ) ) )
            filterTxLog( fileSystem, file, filter, startPosition );
    }
    
    public static void filterTxLog( FileSystemAbstraction fileSystem, File file,
            LogHook<Pair<Byte, List<byte[]>>> filter ) throws IOException
    {
        filterTxLog( fileSystem, file, filter, 0 );
    }
    
    public static void filterTxLog( FileSystemAbstraction fileSystem, File file,
            LogHook<Pair<Byte, List<byte[]>>> filter, long startPosition ) throws IOException
    {
        File tempFile = new File( file.getPath() + ".tmp" );
        FileChannel in = fileSystem.open( file, "r" );
        in.position( startPosition );
        FileChannel out = fileSystem.open( tempFile, "rw" );
        LogBuffer outBuffer = new DirectMappedLogBuffer( out );
        ByteBuffer buffer = ByteBuffer.allocate( 1024*1024 );
        boolean changed = false;
        try
        {
            filter.file( file );
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
                else throw new IllegalArgumentException( "Unknown type:" + type + ", position:" + (in.position()-buffer.remaining()) );
                
                if ( filter.accept( Pair.of( type, xids ) ) )
                {
                    outBuffer.put( type );
                    writeXids( xids, outBuffer );
                }
                else
                    changed = true;
            }
        }
        finally
        {
            safeClose( in );
            outBuffer.force();
            safeClose( out );
            filter.done( file );
        }
        
        if ( changed )
            replace( tempFile, file );
        else
            tempFile.delete();
    }

    public static void assertLogContains( FileSystemAbstraction fileSystem, String logPath,
            LogEntry ... expectedEntries ) throws IOException
    {
        FileChannel fileChannel = fileSystem.open( new File( logPath ), "r" );
        ByteBuffer buffer = ByteBuffer.allocateDirect( 9 + Xid.MAXGTRIDSIZE
                + Xid.MAXBQUALSIZE * 10 );

        try {
            // Always a header
            LogIoUtils.readLogHeader( buffer, fileChannel, true );

            // Read all log entries
            List<LogEntry> entries = new ArrayList<LogEntry>(  );
            CommandFactory cmdFactory = new CommandFactory();
            LogEntry entry;
            while ( (entry = LogIoUtils.readEntry( buffer, fileChannel, cmdFactory )) != null )
            {
                entries.add( entry );
            }

            // Assert entries are what we expected
            for(int entryNo=0;entryNo < expectedEntries.length; entryNo++)
            {
                LogEntry expectedEntry = expectedEntries[entryNo];
                if(entries.size() <= entryNo)
                {
                    fail("Log ended prematurely. Expected to find '" + expectedEntry.toString() + "' as log entry number "+entryNo+", instead there were no more log entries." );
                }

                LogEntry actualEntry = entries.get( entryNo );

                assertThat( "Unexpected entry at entry number " + entryNo, actualEntry, is( expectedEntry ) );
            }

            // And assert log does not contain more entries
            assertThat( "The log contained more entries than we expected!", entries.size(), is( expectedEntries.length ) );
        } finally {
            fileChannel.close();
        }
    }

    
    private static void replace( File tempFile, File file )
    {
        file.renameTo( new File( file.getAbsolutePath() + "." + System.currentTimeMillis() ) );
        tempFile.renameTo( file );
    }

    public static File[] filterNeostoreLogicalLog( FileSystemAbstraction fileSystem,
            String storeDir, LogHook<LogEntry> filter ) throws IOException
    {
        File[] logFiles = oneOrTwo( new File( storeDir, NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME ) );
        for ( File file : logFiles )
        {
            File filteredLog = filterNeostoreLogicalLog( fileSystem, file, filter );
            replace( filteredLog, file );
        }

        return logFiles;
    }

    public static File filterNeostoreLogicalLog( FileSystemAbstraction fileSystem, File file, LogHook<LogEntry> filter )
            throws IOException
    {
        filter.file( file );
        File tempFile = new File( file.getAbsolutePath() + ".tmp" );
        FileChannel in = fileSystem.open( file, "r" );;
        FileChannel out = fileSystem.open( tempFile, "rw" );
        LogBuffer outBuffer = new DirectMappedLogBuffer( out );
        ByteBuffer buffer = ByteBuffer.allocate( 1024*1024 );
        transferLogicalLogHeader( in, outBuffer, buffer );
        CommandFactory cf = new CommandFactory();
        boolean changed = false;
        try
        {
            LogEntry entry = null;
            while ( (entry = readEntry( buffer, in, cf ) ) != null )
            {
                if ( !filter.accept( entry ) )
                {
                    changed = true;
                    continue;
                }
                writeLogEntry( entry, outBuffer );
            }
        }
        finally
        {
            safeClose( in );
            outBuffer.force();
            safeClose( out );
            filter.done( file );
        }

        return tempFile;
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

    private static File[] oneOrTwo( File file )
    {
        List<File> files = new ArrayList<File>();
        File one = new File( file.getAbsolutePath() + ".1" );
        if ( one.exists() ) files.add( one );
        File two = new File( file.getAbsolutePath() + ".2" );
        if ( two.exists() ) files.add( two );
        if ( files.isEmpty() )
            throw new IllegalStateException( "Couldn't find any active tm log" );
        return files.toArray( new File[files.size()] );
    }

    public static NonCleanLogCopy copyLogicalLog( FileSystemAbstraction fileSystem,
            File logBaseFileName ) throws IOException
    {
        char active = '1';
        File activeLog = new File( logBaseFileName.getPath() + ".active" );
        FileChannel af = fileSystem.open( activeLog, "r" );
        ByteBuffer buffer = ByteBuffer.allocate( 1024 );
        af.read( buffer );
        buffer.flip();
        File activeLogBackup = new File( logBaseFileName.getPath() + ".bak.active" );
        FileChannel activeCopy = fileSystem.open( activeLogBackup, "rw" );
        activeCopy.write( buffer );
        activeCopy.close();
        af.close();
        buffer.flip();
        active = buffer.asCharBuffer().get();
        buffer.clear();
        File currentLog = new File( logBaseFileName.getPath() + "." + active );
        FileChannel source = fileSystem.open( currentLog, "r" );
        File currentLogBackup = new File( logBaseFileName.getPath() + ".bak." + active );
        FileChannel dest = fileSystem.open( currentLogBackup, "rw" );
        int read = -1;
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
        return new NonCleanLogCopy(
                new FileBackup( activeLog, activeLogBackup, fileSystem ),
                new FileBackup( currentLog, currentLogBackup, fileSystem ) );
    }
    
    private static class FileBackup
    {
        private final File file, backup;
        private final FileSystemAbstraction fileSystem;
        
        FileBackup( File file, File backup, FileSystemAbstraction fileSystem )
        {
            this.file = file;
            this.backup = backup;
            this.fileSystem = fileSystem;
        }
        
        public void restore() throws IOException
        {
            fileSystem.deleteFile( file );
            fileSystem.renameFile( backup, file );
        }
    }
    
    public static class NonCleanLogCopy
    {
        private final FileBackup[] backups;

        NonCleanLogCopy( FileBackup... backups )
        {
            this.backups = backups;
        }
        
        public void reinstate() throws IOException
        {
            for ( FileBackup backup : backups )
                backup.restore();
        }
    }
}
