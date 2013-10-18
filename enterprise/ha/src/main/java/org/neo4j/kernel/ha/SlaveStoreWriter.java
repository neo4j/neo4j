/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.ServerUtil;
import org.neo4j.com.StoreWriter;
import org.neo4j.com.ToFileStoreWriter;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.ConsoleLogger;

import static org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME;

public class SlaveStoreWriter
{
    public static final String COPY_FROM_MASTER_TEMP = "temp-copy";
    private final Config config;
    private final Iterable<KernelExtensionFactory<?>> kernelExtensions;
    private final ConsoleLogger console;

    // TODO Should be accepted as a dependency
    private final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();

    public SlaveStoreWriter( Config config, Iterable<KernelExtensionFactory<?>> kernelExtensions, ConsoleLogger console )
    {
        this.config = config;
        this.kernelExtensions = kernelExtensions;
        this.console = console;
    }

    public void copyStore( Master master ) throws IOException
    {
        // Clear up the current temp directory if there
        File storeDir = config.get( InternalAbstractGraphDatabase.Configuration.store_dir );
        File tempStore = new File( storeDir, COPY_FROM_MASTER_TEMP );
        if ( !tempStore.mkdir() )
        {
            FileUtils.deleteRecursively( tempStore );
            tempStore.mkdir();
        }

        // Get the response, deserialise to disk
        Response response = master.copyStore( new RequestContext( 0,
                config.get( ClusterSettings.server_id ), 0, new RequestContext.Tx[0], 0,
                0 ), decorateWithProgressIndicator( new ToFileStoreWriter( tempStore ) ) );
        long highestLogVersion = XaLogicalLog.getHighestHistoryLogVersion( fileSystem,
                tempStore, LOGICAL_LOG_DEFAULT_NAME );
        if ( highestLogVersion > -1 )
        {
            NeoStore.setVersion( fileSystem, tempStore, highestLogVersion + 1 );
        }

        GraphDatabaseAPI copiedDb = (GraphDatabaseAPI) new GraphDatabaseFactory()
                .setKernelExtensions( kernelExtensions )
                .newEmbeddedDatabaseBuilder(tempStore.getAbsolutePath() )
                .setConfig(
                    GraphDatabaseSettings.keep_logical_logs, Settings.TRUE ).setConfig(
                    GraphDatabaseSettings.allow_store_upgrade,
                    config.get( GraphDatabaseSettings.allow_store_upgrade ).toString() )

                .newGraphDatabase();

        // Apply pending transactions
        try
        {
            ServerUtil.applyReceivedTransactions( response, copiedDb.getXaDataSourceManager(),
                    ServerUtil.txHandlerForFullCopy() );
        }
        finally
        {
            copiedDb.shutdown();
            response.close();
        }

        // All is well, move to the real store directory
        for ( File candidate : tempStore.listFiles( new FileFilter()
        {
            @Override
            public boolean accept( File file )
            {
                // Skip log files and tx files from temporary database
                return !file.getName().equals( StringLogger.DEFAULT_NAME ) && !("active_tx_log tm_tx_log.1 tm_tx_log" +
                        ".2").contains( file.getName() );
            }
        } ) )
        {
            FileUtils.moveFileToDirectory( candidate, storeDir );
        }
    }

    private StoreWriter decorateWithProgressIndicator( final StoreWriter actual )
    {
        return new StoreWriter()
        {
            private int totalFiles;

            @Override
            public void write( String path, ReadableByteChannel data, ByteBuffer temporaryBuffer,
                               boolean hasData ) throws IOException
            {
                console.log( "Copying " + path );
                actual.write( path, data, temporaryBuffer, hasData );
                console.log( "Copied " + path );
                totalFiles++;
            }

            @Override
            public void done()
            {
                actual.done();
                console.log( "Done, copied " + totalFiles + " files");
            }
        };
    }
}
