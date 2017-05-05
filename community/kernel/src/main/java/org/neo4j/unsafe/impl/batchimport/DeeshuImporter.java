package org.neo4j.unsafe.impl.batchimport;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.BadCollector;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.CsvInput;
import org.neo4j.unsafe.impl.batchimport.input.csv.DataFactory;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository;

import static java.lang.System.currentTimeMillis;
import static java.nio.charset.Charset.defaultCharset;

import static org.neo4j.helpers.Format.duration;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.NO_DECORATOR;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.data;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.datas;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatRelationshipFileHeader;

public class DeeshuImporter
{
    public static void main( String[] args ) throws InterruptedException
    {
        int numRunners = Runtime.getRuntime().availableProcessors();
        Configuration configuration = Configuration.COMMAS;
        File database = new File( "K:/graph.db" );
        try ( DefaultFileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();
              PageCache pageCache = StandalonePageCacheFactory.createPageCache( fileSystemAbstraction );
              Collector collector = new BadCollector( System.out, 0, 0 ); )
        {
            fileSystemAbstraction.deleteRecursively( database );
            DataFactory inputNodeData = data( NO_DECORATOR, defaultCharset(),
                    new File( "K:/csv/nodes.csv" ) );
            IdType idType = IdType.STRING;
            Input input = new CsvInput(
                    // nodes
                    datas( inputNodeData ), defaultFormatNodeFileHeader(),
                    // relationships
                    datas(), defaultFormatRelationshipFileHeader(),
                    idType, configuration, collector );

            try ( NeoStores stores = new StoreFactory( database, pageCache, fileSystemAbstraction,
                    NullLogProvider.getInstance() ).openAllNeoStores( true );
                  BatchingTokenRepository.BatchingPropertyKeyTokenRepository propertyKeyTokenRepository = new
                          BatchingTokenRepository.BatchingPropertyKeyTokenRepository(
                          stores.getPropertyKeyTokenStore() );
                    BatchingTokenRepository.BatchingLabelTokenRepository labelTokenRepository = new
                            BatchingTokenRepository.BatchingLabelTokenRepository(
                            stores.getLabelTokenStore() ); )
            {
                IdMapper idMapper = idType.idMapper();
                long time = currentTimeMillis();
                importNodes( numRunners, input, stores, propertyKeyTokenRepository, labelTokenRepository, idMapper );
                time = currentTimeMillis() - time;
                System.out.println( "Imported in " + duration( time ) );
            }
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }

    private static void importNodes( int numRunners, Input input, NeoStores stores,
            BatchingTokenRepository.BatchingPropertyKeyTokenRepository propertyKeyTokenRepository,
            BatchingTokenRepository.BatchingLabelTokenRepository labelTokenRepository, IdMapper idMapper )
                    throws InterruptedException
    {
        ExecutorService pool = Executors.newFixedThreadPool( numRunners );
        InputIterator nodes = input.nodes().iterator();
        for ( int i = 0; i < numRunners; i++ )
        {
            pool.submit( new NodeImporter( nodes, stores, propertyKeyTokenRepository,
                    labelTokenRepository, idMapper ) );
        }
        pool.shutdown();
        pool.awaitTermination( 100, TimeUnit.DAYS );
    }
}
