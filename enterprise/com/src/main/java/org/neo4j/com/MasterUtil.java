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
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Exceptions;
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
import org.neo4j.kernel.impl.transaction.xaframework.InMemoryLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogExtractor;
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
    
    private static String relativePath( File baseDir, File storeFile )
            throws IOException
    {
        String prefix = baseDir.getCanonicalPath();
        String path = storeFile.getCanonicalPath();
        if ( !path.startsWith( prefix ) )
            throw new FileNotFoundException();
        path = path.substring( prefix.length() );
        if ( path.startsWith( File.separator ) )
            return path.substring( 1 );
        return path;
    }
    
    public static Pair<String, Long>[] rotateLogs( GraphDatabaseService graphDb )
    {
        XaDataSourceManager dsManager =
                ((AbstractGraphDatabase) graphDb).getConfig().getTxModule().getXaDataSourceManager();
        Collection<XaDataSource> sources = dsManager.getAllRegisteredDataSources();
        
        @SuppressWarnings( "unchecked" )
        Pair<String, Long>[] appliedTransactions = new Pair[sources.size()];
        int i = 0;
        for ( XaDataSource ds : sources )
        {
            try
            {
                long lastCommittedTx = ds.getXaContainer().getResourceManager().rotateLogicalLog();
                appliedTransactions[i++] = Pair.of( ds.getName(), lastCommittedTx );
            }
            catch ( IOException e )
            {
                // TODO: what about error message?
                ((AbstractGraphDatabase)graphDb).getMessageLog().logMessage(
                        "Unable to rotate log for " + ds, e );
                throw new MasterFailureException( e );
            }
        }
        return appliedTransactions;
    }

    public static SlaveContext rotateLogsAndStreamStoreFiles( GraphDatabaseService graphDb,
            boolean includeLogicalLogs, StoreWriter writer )
    {
        File baseDir = getBaseDir( graphDb );
        XaDataSourceManager dsManager =
                ((AbstractGraphDatabase) graphDb).getConfig().getTxModule().getXaDataSourceManager();
        SlaveContext context = SlaveContext.anonymous( rotateLogs( graphDb ) );
        ByteBuffer temporaryBuffer = ByteBuffer.allocateDirect( 1024*1024 );
        for ( XaDataSource ds : dsManager.getAllRegisteredDataSources() )
        {
            try
            {
                ClosableIterable<File> files = ds.listStoreFiles( includeLogicalLogs );
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
        final List<LogExtractor> logExtractors = new ArrayList<LogExtractor>();
        try
        {
            for ( Pair<String, Long> txEntry : context.lastAppliedTransactions() )
            {
                String resourceName = txEntry.first();
                final XaDataSource dataSource = dsManager.getXaDataSource( resourceName );
                if ( dataSource == null )
                {
                    throw new RuntimeException( "No data source '" + resourceName + "' found" );
                }
                resourceNames.add( resourceName );
                final long masterLastTx = dataSource.getLastCommittedTxId();
                LogExtractor logExtractor;
                try
                {
                    logExtractor = dataSource.getLogExtractor( txEntry.other()+1, masterLastTx );
                }
                catch ( IOException ioe )
                {
                    throw new RuntimeException( ioe );
                }
                final LogExtractor finalLogExtractor = logExtractor;
                logExtractors.add( finalLogExtractor );
                final long startTxId = txEntry.other() + 1;
                for ( long txId = startTxId; txId <= masterLastTx; txId++ )
                {
                    if ( filter.accept( txId ) )
                    {
                        final long tx = txId;
                        TxExtractor extractor = new TxExtractor()
                        {
                            @Override
                            public ReadableByteChannel extract()
                            {
                                InMemoryLogBuffer buffer = new InMemoryLogBuffer();
                                extract( buffer );
                                return buffer;
                            }
    
                            @Override
                            public void extract( LogBuffer buffer )
                            {
                                try
                                {
                                    long extractedTxId = finalLogExtractor.extractNext( buffer );
                                    if ( extractedTxId == -1 )
                                    {
                                        throw new RuntimeException( "Transaction " + tx +
                                                " is missing and can't be extracted from " +
                                                dataSource.getName() + ". Was about to extract " +
                                                startTxId + " to " + masterLastTx );
                                    }
                                    if ( extractedTxId != tx )
                                    {
                                        throw new RuntimeException( "Expected txId " + tx +
                                                ", but was " + extractedTxId );
                                    }
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
            return new Response<T>( response, storeId, createTransactionStream( resourceNames, stream, logExtractors ) );
        }
        catch ( Throwable t )
        {   // If there's an error in here then close the log extractors, otherwise if we're
            // successful the TransactionStream will close it.
            for ( LogExtractor extractor : logExtractors ) extractor.close();
            throw Exceptions.launderedException( t );
        }
    }
    
    private static TransactionStream createTransactionStream( Collection<String> resourceNames,
            final List<Triplet<String, Long, TxExtractor>> stream, final List<LogExtractor> logExtractors )
    {
        return new TransactionStream( resourceNames.toArray( new String[resourceNames.size()] ) )
        {
            private final Iterator<Triplet<String, Long, TxExtractor>> iterator = stream.iterator();
            
            @Override
            protected Triplet<String, Long, TxExtractor> fetchNextOrNull()
            {
                return iterator.hasNext() ? iterator.next() : null;
            }
            
            @Override
            public void close()
            {
                for ( LogExtractor extractor : logExtractors ) extractor.close();
            }
        };
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
        @Override
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
        @Override
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
            
            @Override
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
