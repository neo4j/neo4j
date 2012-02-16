/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.neo4j.com.SlaveContext.lastAppliedTx;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.com.SlaveContext.Tx;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.helpers.Exceptions;
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

    /**
     * Given a directory and a path under it, return filename of the path
     * relative to the directory.
     *
     * @param baseDir The base directory, containing the storeFile
     * @param storeFile The store file path, must be contained under
     *            <code>baseDir</code>
     * @return The relative path of <code>storeFile</code> to
     *         <code>baseDir</code>
     * @throws IOException As per {@link File#getCanonicalPath()}
     */
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

    public static Tx[] rotateLogs( GraphDatabaseService graphDb )
    {
        XaDataSourceManager dsManager =
                ((AbstractGraphDatabase) graphDb).getConfig().getTxModule().getXaDataSourceManager();
        Collection<XaDataSource> sources = dsManager.getAllRegisteredDataSources();

        Tx[] appliedTransactions = new Tx[sources.size()];
        int i = 0;
        for ( XaDataSource ds : sources )
        {
            try
            {
                appliedTransactions[i++] = lastAppliedTx( ds.getName(), ds.getXaContainer().getResourceManager().rotateLogicalLog() );
            }
            catch ( Throwable e )
            {   // This must be treated as a kernel panic, failure to rotate is bad.
                ((AbstractGraphDatabase)graphDb).getMessageLog().logMessage(
                        "Unable to rotate log for " + ds, e );
                // TODO If we do it in rotate() the transaction semantics for such a failure will change
                // slightly and that has got to be verified somehow. But to have it in there feels much better.
                ((AbstractGraphDatabase)graphDb).getConfig().getKernelPanicGenerator().generateEvent( ErrorState.TX_MANAGER_NOT_OK );
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

    /**
     * For a given {@link XaDataSource} it extracts the transaction stream from
     * startTxId up to endTxId (inclusive) in the provided {@link List} and
     * returns the {@link LogExtractor} used to create the stream.
     *
     * @param dataSource The {@link XaDataSource} from which to extract the
     *            transactions
     * @param startTxId The first tx id in the stream
     * @param endTxId The last tx id in the stream
     * @param stream A list to contain the transaction stream - can already
     *            contain transactions from other data sources.
     * @return The {@link LogExtractor} used to create the transaction stream.
     */
    private static LogExtractor getTransactionStreamForDatasource(
            final XaDataSource dataSource, final long startTxId,
            final long endTxId,
            final List<Triplet<String, Long, TxExtractor>> stream,
            Predicate<Long> filter )
    {
        LogExtractor logExtractor = null;
        try
        {
            final long masterLastTx = dataSource.getLastCommittedTxId();
            if ( masterLastTx < endTxId )
            {
                throw new RuntimeException(
                        "Was requested to extract transaction ids " + startTxId
                                + " to " + endTxId + " from data source "
                                + dataSource.getName()
                                + " but largest transaction id in master is "
                                + masterLastTx );
            }
            try
            {
                // TODO check here for startTxId >= endTxId and exit early
                logExtractor = dataSource.getLogExtractor( startTxId, endTxId );
            }
            catch ( IOException ioe )
            {
                throw new RuntimeException( ioe );
            }
            final LogExtractor finalLogExtractor = logExtractor;
            for ( long txId = startTxId; txId <= endTxId; txId++ )
            {
                if ( filter.accept( txId ) )
                {
                    final long finalTxId = txId;
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
                                    throw new RuntimeException(
                                            "Transaction "
                                                    + finalTxId
                                                    + " is missing and can't be extracted from "
                                                    + dataSource.getName()
                                                    + ". Was about to extract "
                                                    + startTxId + " to "
                                                    + endTxId );
                                }
                                if ( extractedTxId != finalTxId )
                                {
                                    throw new RuntimeException(
                                            "Expected txId " + finalTxId
                                                    + ", but was "
                                                    + extractedTxId );
                                }
                            }
                            catch ( IOException e )
                            {
                                throw new RuntimeException( e );
                            }
                        }
                    };
                    stream.add( Triplet.of( dataSource.getName(), txId,
                            extractor ) );
                }
            }
            return logExtractor;
        }
        catch ( Throwable t )
        {
            /*
             * If there's an error in here then close the log extractors,
             * otherwise if we're successful the TransactionStream will close it.
             */
            if ( logExtractor != null ) logExtractor.close();
            throw Exceptions.launderedException( t );
        }
    }

    /**
     * After having created the response for a slave, this method compares its
     * context against the local (master's) context and creates a transaction
     * stream containing all the transactions the slave does not currently
     * have. This way every response returned acts as an update for the slave.
     *
     * @param <T> The type of the response
     * @param graphDb The graph database to use
     * @param context The slave context
     * @param response The response being packed
     * @param filter A {@link Predicate} to apply on each txid, selecting only
     *            those that evaluate to true
     * @return The response, packed with the latest transactions
     */
    public static <T> Response<T> packResponse( GraphDatabaseService graphDb,
            SlaveContext context, T response, Predicate<Long> filter )
    {
        List<Triplet<String, Long, TxExtractor>> stream = new ArrayList<Triplet<String, Long, TxExtractor>>();
        Set<String> resourceNames = new HashSet<String>();
        XaDataSourceManager dsManager = ((AbstractGraphDatabase) graphDb).getConfig().getTxModule().getXaDataSourceManager();
        final List<LogExtractor> logExtractors = new ArrayList<LogExtractor>();
        try
        {
            for ( Tx txEntry : context.lastAppliedTransactions() )
            {
                String resourceName = txEntry.getDataSourceName();
                final XaDataSource dataSource = dsManager.getXaDataSource( resourceName );
                if ( dataSource == null )
                {
                    throw new RuntimeException( "No data source '" + resourceName + "' found" );
                }
                resourceNames.add( resourceName );
                final long masterLastTx = dataSource.getLastCommittedTxId();
                if ( txEntry.getTxId() >= masterLastTx ) continue;
                LogExtractor logExtractor = getTransactionStreamForDatasource(
                        dataSource, txEntry.getTxId() + 1, masterLastTx, stream,
                        filter );
                logExtractors.add( logExtractor );
            }
            StoreId storeId = ((NeoStoreXaDataSource) dsManager.getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME )).getStoreId();
            return new Response<T>( response, storeId, createTransactionStream(
                    resourceNames, stream, logExtractors ),
                    ResourceReleaser.NO_OP );
        }
        catch ( Throwable t )
        {   // If there's an error in here then close the log extractors, otherwise if we're
            // successful the TransactionStream will close it.
            for ( LogExtractor extractor : logExtractors ) extractor.close();
            throw Exceptions.launderedException( t );
        }
    }

    /**
     * Given a data source name, a start and an end tx, this method extracts
     * these transactions (inclusive) in a transaction stream and encapsulates
     * them in a {@link Response} object, ready to be returned to the slave.
     *
     * @param graphDb The graph database to use
     * @param dataSourceName The name of the data source to extract transactions
     *            from
     * @param startTx The first tx in the returned stream
     * @param endTx The last tx in the returned stream
     * @return A {@link Response} object containing a transaction stream with
     *         the requested transactions from the specified data source.
     */
    public static Response<Void> getTransactions( GraphDatabaseService graphDb,
            String dataSourceName, long startTx, long endTx )
    {
        List<Triplet<String, Long, TxExtractor>> stream = new ArrayList<Triplet<String, Long, TxExtractor>>();
        XaDataSourceManager dsManager = ( (AbstractGraphDatabase) graphDb ).getConfig().getTxModule().getXaDataSourceManager();
        final XaDataSource dataSource = dsManager.getXaDataSource( dataSourceName );
        if ( dataSource == null )
        {
            throw new RuntimeException( "No data source '" + dataSourceName
                                        + "' found" );
        }

        List<LogExtractor> extractors = startTx < endTx ? Collections.singletonList(
                getTransactionStreamForDatasource( dataSource, startTx, endTx, stream, MasterUtil.ALL ) ) :
                Collections.<LogExtractor>emptyList();
        StoreId storeId = ( (NeoStoreXaDataSource) dsManager.getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME ) ).getStoreId();
        return new Response<Void>( null, storeId, createTransactionStream(
                        Collections.singletonList( dataSourceName ), stream,
                        extractors ), ResourceReleaser.NO_OP );

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
        return new Response<T>( response, storeId, TransactionStream.EMPTY,
                ResourceReleaser.NO_OP );
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
        try
        {
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
        finally
        {
            response.close();
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
