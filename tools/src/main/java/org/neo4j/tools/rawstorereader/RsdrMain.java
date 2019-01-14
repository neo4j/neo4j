/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.tools.rawstorereader;

import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.cursor.IOCursor;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.string.HexString;
import org.neo4j.tools.util.TransactionLogUtils;

import static org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory.createPageCache;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;

/**
 * Tool to read raw data from various stores.
 */
public class RsdrMain
{

    private static final Console console = System.console();
    private static final Pattern readCommandPattern = Pattern.compile( "r" + // 'r' means read command
            "((?<lower>\\d+)?,(?<upper>\\d+)?)?\\s+" + // optional record id range bounds, followed by whitespace
            "(?<fname>[\\w.]+)" + // files are a sequence of word characters or literal '.'
            "(\\s*\\|\\s*(?<regex>.+))?" // a pipe signifies a regex to filter records by
    );

    private RsdrMain()
    {
    }

    public static void main( String[] args ) throws IOException
    {
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            console.printf( "Neo4j Raw Store Diagnostics Reader%n" );

            if ( args.length != 1 || !fileSystem.isDirectory( new File( args[0] ) ) )
            {
                console.printf( "Usage: rsdr <store directory>%n" );
                return;
            }

            File storedir = new File( args[0] );

            Config config = buildConfig();
            try ( PageCache pageCache = createPageCache( fileSystem, config ) )
            {
                File neoStore = new File( storedir, MetaDataStore.DEFAULT_NAME );
                StoreFactory factory = openStore( fileSystem, neoStore, config, pageCache );
                NeoStores neoStores = factory.openAllNeoStores();
                interact( fileSystem, neoStores );
            }
        }
    }

    private static Config buildConfig()
    {
        return Config.defaults( MapUtil.stringMap(
                GraphDatabaseSettings.read_only.name(), "true",
                GraphDatabaseSettings.pagecache_memory.name(), "64M"
        ) );
    }

    private static StoreFactory openStore( FileSystemAbstraction fileSystem, File storeDir, Config config,
            PageCache pageCache )
    {
        IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem );
        NullLogProvider logProvider = NullLogProvider.getInstance();
        return new StoreFactory( storeDir, config, idGeneratorFactory, pageCache, fileSystem, logProvider, EmptyVersionContextSupplier.EMPTY );
    }

    private static void interact( FileSystemAbstraction fileSystem, NeoStores neoStores ) throws IOException
    {
        printHelp();

        String cmd;
        do
        {
            cmd = console.readLine( "neo? " );
        } while ( execute( fileSystem, cmd, neoStores ) );
        System.exit( 0 );
    }

    private static void printHelp()
    {
        console.printf( "Usage:%n" +
                "  h            print this message%n" +
                "  l            list store files in store%n" +
                "  r f          read all records in store file 'f'%n" +
                "  r5,10 f      read record 5 through 10 in store file 'f'%n" +
                "  r f | rx     read records and filter through regex 'rx'%n" +
                "  q            quit%n" );
    }

    private static boolean execute( FileSystemAbstraction fileSystem, String cmd, NeoStores neoStores )
            throws IOException
    {
        if ( cmd == null || cmd.equals( "q" ) )
        {
            return false;
        }
        else if ( cmd.equals( "h" ) )
        {
            printHelp();
        }
        else if ( cmd.equals( "l" ) )
        {
            listFiles( fileSystem, neoStores );
        }
        else if ( cmd.startsWith( "r" ) )
        {
            read( fileSystem, cmd, neoStores );
        }
        else if ( !cmd.trim().isEmpty() )
        {
            console.printf( "unrecognized command%n" );
        }
        return true;
    }

    private static void listFiles( FileSystemAbstraction fileSystem, NeoStores neoStores )
    {
        File storedir = neoStores.getStoreDir();
        File[] listing = fileSystem.listFiles( storedir );
        for ( File file : listing )
        {
            console.printf( "%s%n", file.getName() );
        }
    }

    private static void read( FileSystemAbstraction fileSystem, String cmd, NeoStores neoStores ) throws IOException
    {
        Matcher matcher = readCommandPattern.matcher( cmd );
        if ( matcher.find() )
        {
            String lower = matcher.group( "lower" );
            String upper = matcher.group( "upper" );
            String fname = matcher.group( "fname" );
            String regex = matcher.group( "regex" );
            Pattern pattern = regex != null ? Pattern.compile( regex ) : null;
            long fromId = lower != null ? Long.parseLong( lower ) : 0L;
            long toId = upper != null ? Long.parseLong( upper ) : Long.MAX_VALUE;

            RecordStore store = getStore( fname, neoStores );
            if ( store != null )
            {
                readStore( fileSystem, store, fromId, toId, pattern );
                return;
            }

            IOCursor<LogEntry> cursor = getLogCursor( fileSystem, fname, neoStores );
            if ( cursor != null )
            {
                readLog( cursor, fromId, toId, pattern );
                cursor.close();
                return;
            }

            console.printf( "don't know how to read '%s'%n", fname );
        }
        else
        {
            console.printf( "bad read command format%n" );
        }
    }

    private static void readStore( FileSystemAbstraction fileSystem,
            RecordStore store, long fromId, long toId, Pattern pattern ) throws IOException
    {
        toId = Math.min( toId, store.getHighId() );
        try ( StoreChannel channel = fileSystem.open( store.getStorageFileName(), OpenMode.READ ) )
        {
            int recordSize = store.getRecordSize();
            ByteBuffer buf = ByteBuffer.allocate( recordSize );
            for ( long i = fromId; i <= toId; i++ )
            {
                buf.clear();
                long offset = recordSize * i;
                int count = channel.read( buf, offset );
                if ( count == -1 )
                {
                    break;
                }
                byte[] bytes = new byte[count];
                buf.clear();
                buf.get( bytes );
                String hex = HexString.encodeHexString( bytes );
                int paddingNeeded = (recordSize * 2 - Math.max( count * 2, 0 )) + 1;
                String format = "%s %6s 0x%08X %s%" + paddingNeeded + "s%s%n";
                String str;
                String use;

                try
                {
                    AbstractBaseRecord record = RecordStore.getRecord( store, i, CHECK );
                    use = record.inUse() ? "+" : "-";
                    str = record.toString();
                }
                catch ( InvalidRecordException e )
                {
                    str = new String( bytes, 0, count, "ASCII" );
                    use = "?";
                }

                if ( pattern == null || pattern.matcher( str ).find() )
                {
                    console.printf( format, use, i, offset, hex, " ", str );
                }
            }
        }
    }

    private static RecordStore getStore( String fname, NeoStores neoStores )
    {
        switch ( fname )
        {
        case "neostore.nodestore.db":
            return neoStores.getNodeStore();
        case "neostore.labeltokenstore.db":
            return neoStores.getLabelTokenStore();
        case "neostore.propertystore.db.index":
            return neoStores.getPropertyKeyTokenStore();
        case "neostore.propertystore.db":
            return neoStores.getPropertyStore();
        case "neostore.relationshipgroupstore.db":
            return neoStores.getRelationshipGroupStore();
        case "neostore.relationshipstore.db":
            return neoStores.getRelationshipStore();
        case "neostore.relationshiptypestore.db":
            return neoStores.getRelationshipTypeTokenStore();
        case "neostore.schemastore.db":
            return neoStores.getSchemaStore();
        default:
            return null;
        }
    }

    private static IOCursor<LogEntry> getLogCursor( FileSystemAbstraction fileSystem, String fname,
            NeoStores neoStores ) throws IOException
    {
        return TransactionLogUtils
                .openLogEntryCursor( fileSystem, new File( neoStores.getStoreDir(), fname ), NO_MORE_CHANNELS );
    }

    private static void readLog(
            IOCursor<LogEntry> cursor,
            final long fromLine,
            final long toLine,
            final Pattern pattern ) throws IOException
    {
        TimeZone timeZone = TimeZone.getDefault();
        long lineCount = -1;
        while ( cursor.next() )
        {
            LogEntry logEntry = cursor.get();
            lineCount++;
            if ( lineCount > toLine )
            {
                return;
            }
            if ( lineCount < fromLine )
            {
                continue;
            }
            String str = logEntry.toString( timeZone );
            if ( pattern == null || pattern.matcher( str ).find() )
            {
                console.printf( "%s%n", str );
            }
        }
    }
}
