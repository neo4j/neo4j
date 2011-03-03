/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.com;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Triplet;
import org.neo4j.helpers.collection.ClosableIterable;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public class MasterUtil
{
    private static File getBaseDir( GraphDatabaseService graphDb )
    {
        File file = new File( ((AbstractGraphDatabase) graphDb).getStoreDir() );
        try
        {
            return file.getCanonicalFile().getAbsoluteFile();
        }
        catch ( IOException e )
        {
            return file.getAbsoluteFile();
        }
    }
    
    private static String relativePath( File baseDir, File storeFile ) throws FileNotFoundException
    {
        String prefix = baseDir.getAbsolutePath();
        String path = storeFile.getAbsolutePath();
        if ( !path.startsWith( prefix ) )
            throw new FileNotFoundException();
        path = path.substring( prefix.length() );
        if ( path.startsWith( "/" ) )
            return path.substring( 1 );
        return path;
    }
    
    public static SlaveContext rotateLogsAndStreamStoreFiles( GraphDatabaseService graphDb, StoreWriter writer )
    {
        if ( Config.osIsWindows() )
        {
            throw new UnsupportedOperationException(
                "Streaming store files live (as used in HA and backup) "
                    + "isn't supported on Windows due to limitations in OS/filesystem" );
        }

        File baseDir = getBaseDir( graphDb );
        XaDataSourceManager dsManager =
                ((AbstractGraphDatabase) graphDb).getConfig().getTxModule().getXaDataSourceManager();
        Collection<XaDataSource> sources = dsManager.getAllRegisteredDataSources();
        
        @SuppressWarnings( "unchecked" )
        Pair<String, Long>[] appliedTransactions = new Pair[sources.size()];
        int i = 0;
        for ( XaDataSource ds : sources )
        {
            appliedTransactions[i++] = Pair.of( ds.getName(), ds.getLastCommittedTxId() );
            try
            {
                ds.getXaContainer().getResourceManager().rotateLogicalLog();
            }
            catch ( IOException e )
            {
                // TODO: what about error message?
                throw new MasterFailureException( e );
            }
        }
        SlaveContext context = new SlaveContext( -1, -1, appliedTransactions );
        
        ByteBuffer temporaryBuffer = ByteBuffer.allocateDirect( 1024*1024 );
        for ( XaDataSource ds : sources )
        {
            try
            {
                ClosableIterable<File> files = ds.listStoreFiles();
                try
                {
                    for ( File storefile : files )
                    {
                        FileInputStream stream = new FileInputStream( storefile );
                        try
                        {
                            writer.write( relativePath( baseDir, storefile ), stream.getChannel(), temporaryBuffer,
                                    storefile.length() > 0 );
                        }
                        finally
                        {
                            stream.close();
                        }
                    }
                }
                finally
                {
                    files.close();
                }
            }
            catch ( IOException e )
            {
                // TODO: what about error message?
                throw new MasterFailureException( e );
            }
        }
        return context;
    }
    
    public static <T> Response<T> packResponse( GraphDatabaseService graphDb,
            SlaveContext context, T response, Predicate<Long> filter )
    {
        List<Triplet<String, Long, TxExtractor>> stream = new ArrayList<Triplet<String, Long, TxExtractor>>();
        Set<String> resourceNames = new HashSet<String>();
        XaDataSourceManager dsManager = ((AbstractGraphDatabase) graphDb).getConfig().getTxModule().getXaDataSourceManager();
        for ( Pair<String, Long> txEntry : context.lastAppliedTransactions() )
        {
            String resourceName = txEntry.first();
            final XaDataSource dataSource = dsManager.getXaDataSource( resourceName );
            if ( dataSource == null )
            {
                throw new RuntimeException( "No data source '" + resourceName + "' found" );
            }
            resourceNames.add( resourceName );
            long masterLastTx = dataSource.getLastCommittedTxId();
            for ( long txId = txEntry.other() + 1; txId <= masterLastTx; txId++ )
            {
                if ( filter.accept( txId ) )
                {
                    final long tx = txId;
                    TxExtractor extractor = new TxExtractor()
                    {
                        @Override
                        public ReadableByteChannel extract()
                        {
                            try
                            {
                                return dataSource.getCommittedTransaction( tx );
                            }
                            catch ( IOException e )
                            {
                                throw new RuntimeException( e );
                            }
                        }

                        @Override
                        public void extract( LogBuffer buffer )
                        {
                            try
                            {
                                dataSource.getCommittedTransaction( tx, buffer );
                            }
                            catch ( IOException e )
                            {
                                throw new RuntimeException( e );
                            }
                        }
                    };
                    stream.add( Triplet.of( resourceName, txId, extractor ) );
                }
            }
        }
        StoreId storeId = ((NeoStoreXaDataSource) dsManager.getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME )).getStoreId();
        return new Response<T>( response, storeId, TransactionStream.create( resourceNames, stream ) );
    }
    
    public static <T> Response<T> packResponseWithoutTransactionStream( GraphDatabaseService graphDb,
            SlaveContext context, T response )
    {
        XaDataSource ds = ((AbstractGraphDatabase) graphDb).getConfig().getTxModule()
                .getXaDataSourceManager().getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
        StoreId storeId = ((NeoStoreXaDataSource) ds).getStoreId();
        return new Response<T>( response, storeId, TransactionStream.EMPTY );
    }

    public static final Predicate<Long> ALL = new Predicate<Long>()
    {
        public boolean accept( Long item )
        {
            return true;
        }
    };

    public static <T> void applyReceivedTransactions( Response<T> response, GraphDatabaseService graphDb, TxHandler txHandler ) throws IOException
    {
        XaDataSourceManager dataSourceManager = ((AbstractGraphDatabase) graphDb).getConfig().getTxModule().getXaDataSourceManager();
        for ( Triplet<String, Long, TxExtractor> tx : IteratorUtil.asIterable( response.transactions() ) )
        {
            String resourceName = tx.first();
            XaDataSource dataSource = dataSourceManager.getXaDataSource( resourceName );
            txHandler.accept( tx, dataSource );
            ReadableByteChannel txStream = tx.third().extract();
            try
            {
                dataSource.applyCommittedTransaction( tx.second(), txStream );
            }
            finally
            {
                txStream.close();
            }
        }
    }

    public interface TxHandler
    {
        void accept( Triplet<String, Long, TxExtractor> tx, XaDataSource dataSource );
    }
    
    public static final TxHandler NO_ACTION = new TxHandler()
    {
        public void accept( Triplet<String, Long, TxExtractor> tx, XaDataSource dataSource )
        {
            // Do nothing
        }
    };
    
    public static TxHandler txHandlerForFullCopy()
    {
        return new TxHandler()
        {
            private final Set<String> visitedDataSources = new HashSet<String>();
            
            public void accept( Triplet<String, Long, TxExtractor> tx, XaDataSource dataSource )
            {
                if ( visitedDataSources.add( tx.first() ) )
                {
                    dataSource.setLastCommittedTxId( tx.second()-1 );
                }
            }
        };
    }
}
