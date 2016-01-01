/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package upgrade;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.neo4j.index.impl.lucene.LuceneDataSource;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.nioneo.xa.LogDeserializer;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandReader;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandReaderFactory;
import org.neo4j.kernel.impl.nioneo.xa.command.PhysicalLogNeoXaCommandReaderV1;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.util.Consumer;
import org.neo4j.kernel.impl.util.Cursor;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.ha.ClusterManager;

import static org.neo4j.test.ha.ClusterManager.clusterOfSize;

public class StoreMigratorTestUtil
{
    StoreMigratorTestUtil()
    {
        // no istance allowed
    }

    public static ClusterManager.ManagedCluster buildClusterWithMasterDirIn( FileSystemAbstraction fs,
                                                                       final File legacyStoreDir,
                                                                       CleanupRule cleanup )
            throws Throwable
    {
        File haRootDir = new File( legacyStoreDir.getParentFile(), "ha-migration" );
        fs.deleteRecursively( haRootDir );

        ClusterManager clusterManager = new ClusterManager.Builder( haRootDir )
                .withStoreDirInitializer( new ClusterManager.StoreDirInitializer()
                {
                    @Override
                    public void initializeStoreDir( int serverId, File storeDir ) throws IOException
                    {
                        if ( serverId == 1 ) // Initialize dir only for master, others will copy store from it
                        {
                            FileUtils.copyRecursively( legacyStoreDir, storeDir );
                        }
                    }
                } )
                .withProvider( clusterOfSize( 3 ) )
                .build();

        clusterManager.start();

        cleanup.add( clusterManager );

        return clusterManager.getDefaultCluster();
    }

    public static File[] findAllMatchingFiles( File baseDir, String regex )
    {
        final Pattern pattern = Pattern.compile( regex );
        File[] files = baseDir.listFiles( new FilenameFilter()
        {
            @Override
            public boolean accept( File dir, String name )
            {
                return pattern.matcher( name ).matches();
            }
        } );
        Arrays.sort( files );
        return files;
    }

    public static List<LogEntry> readTransactionLogEntriesFrom( FileSystemAbstraction fs, File file ) throws IOException
    {
        return readAllLogEntries( fs, file, new XaCommandReaderFactory()
        {
            @Override
            public XaCommandReader newInstance( byte logEntryVersion, ByteBuffer scratch )
            {
                return new PhysicalLogNeoXaCommandReaderV1( scratch );
            }
        } );
    }

    public static List<LogEntry> readLuceneLogEntriesFrom( FileSystemAbstraction fs, File file ) throws IOException
    {
        return readAllLogEntries( fs, file, new LuceneDataSource.LuceneCommandReaderFactory( null, null ) );
    }

    private static List<LogEntry> readAllLogEntries( FileSystemAbstraction fs,
                                                     File file,
                                                     XaCommandReaderFactory cmdReaderFactory )
            throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( 1000 );
        LogDeserializer logDeserializer = new LogDeserializer( buffer, cmdReaderFactory );

        final List<LogEntry> logs = new ArrayList<>();
        Consumer<LogEntry, IOException> consumer = new Consumer<LogEntry, IOException>()
        {
            @Override
            public boolean accept( LogEntry entry ) throws IOException
            {
                return logs.add( entry );
            }
        };

        try ( StoreChannel channel = fs.open( file, "r" );
              Cursor<LogEntry, IOException> cursor = logDeserializer.cursor( channel ) )
        {
            VersionAwareLogEntryReader.readLogHeader( buffer, channel, false );
            while ( cursor.next( consumer ) );
        }

        return logs;
    }
}
