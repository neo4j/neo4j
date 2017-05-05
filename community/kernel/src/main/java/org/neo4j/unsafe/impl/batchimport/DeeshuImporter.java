package org.neo4j.unsafe.impl.batchimport;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.csv.reader.CharReadableChunker;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.unsafe.impl.batchimport.input.BadCollector;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.Data;
import org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository;

import static java.nio.charset.Charset.defaultCharset;

import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.NO_DECORATOR;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.data;

public class DeeshuImporter
{
    public static void main( String[] args ) throws InterruptedException
    {
        int numRunners = Runtime.getRuntime().availableProcessors();
        Configuration configuration = Configuration.COMMAS;
        File database = new File( "K:/graph.db" );
        try ( DefaultFileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();
              PageCache pageCache = StandalonePageCacheFactory.createPageCache( fileSystemAbstraction ); )
        {
            fileSystemAbstraction.deleteRecursively( database );
            Data inputNodeData = data( NO_DECORATOR, defaultCharset(),
                    new File( "/home/ragnar/csv/nodes.csv" ) ).create( configuration );

            try ( NeoStores stores = new StoreFactory( database, pageCache, fileSystemAbstraction,
                    NullLogProvider.getInstance() ).openAllNeoStores( true );
                  BatchingTokenRepository.BatchingPropertyKeyTokenRepository propertyKeyTokenRepository = new
                          BatchingTokenRepository.BatchingPropertyKeyTokenRepository(
                          stores.getPropertyKeyTokenStore() );
                  CharReadableChunker processingSource = new CharReadableChunker( inputNodeData.stream(), 409600 ); )
            {
                doImport( numRunners, inputNodeData, processingSource, stores, propertyKeyTokenRepository,
                        configuration );
            }
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }

    private static void doImport( int numRunners, Data inputNodeData, CharReadableChunker processingSource,
            NeoStores stores, BatchingTokenRepository.BatchingPropertyKeyTokenRepository propertyKeyTokenRepository,
            Configuration configuration ) throws InterruptedException
    {
        ExecutorService pool = Executors.newFixedThreadPool( numRunners );

        Collector collector = new BadCollector( System.err, 0, 0 );
        for ( int i = 0; i < numRunners; i++ )
        {
            pool.submit( new NodeImporter( processingSource, stores, configuration, collector,
                    propertyKeyTokenRepository ) );

        }
        pool.shutdown();
        pool.awaitTermination( 100, TimeUnit.DAYS );
    }
}
