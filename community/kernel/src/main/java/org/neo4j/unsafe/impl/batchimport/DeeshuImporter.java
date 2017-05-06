/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.BadCollector;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.CsvInput;
import org.neo4j.unsafe.impl.batchimport.input.csv.DataFactory;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;
import static java.lang.System.currentTimeMillis;
import static java.nio.charset.Charset.defaultCharset;

import static org.neo4j.helpers.Format.duration;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
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
              Collector collector = new BadCollector( System.out, 0, 0 ); )
        {
            fileSystemAbstraction.deleteRecursively( database );
            DataFactory nodeData = data( NO_DECORATOR, defaultCharset(), new File( "K:/csv/nodes.csv" ) );
            DataFactory relationshipData = data( NO_DECORATOR, defaultCharset(),
                    new File( "K:/csv/relationships2.csv" ) );
            IdType idType = IdType.STRING;
            Input input = new CsvInput(
                    // nodes
                    datas( nodeData ), defaultFormatNodeFileHeader(),
                    // relationships
                    datas( relationshipData ), defaultFormatRelationshipFileHeader(),
                    idType, configuration, collector );

            Config config = Config.defaults();
            RecordFormats format = RecordFormatSelector.selectForConfig( config, NullLogProvider.getInstance() );
            try ( BatchingNeoStores stores = BatchingNeoStores.batchingNeoStores(
                    fileSystemAbstraction, database, format, DEFAULT, NullLogService.getInstance(), EMPTY, config ) )
            {
                IdMapper idMapper = idType.idMapper();
                long time = currentTimeMillis();
                stores.startFlushingPageCache();
                importNodes( numRunners, input, stores, idMapper );
                stores.stopFlushingPageCache();
                idMapper.prepare( new NodeInputIdPropertyLookup( stores.getNodeStore(), stores.getPropertyStore() ),
                        collector, ProgressListener.NONE );
                stores.startFlushingPageCache();
                importRelationships( numRunners, input, stores, idMapper );
                stores.stopFlushingPageCache();
                time = currentTimeMillis() - time;
                System.out.println( "Imported in " + duration( time ) );
            }
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }

    private static void importData( int numRunners, InputIterator data, Supplier<InputEntityVisitor> visitors )
            throws InterruptedException
    {
        ExecutorService pool = Executors.newFixedThreadPool( numRunners );
        for ( int i = 0; i < numRunners; i++ )
        {
            pool.submit( new ExhaustingInputVisitorRunnable( data, visitors.get() ) );
        }
        pool.shutdown();
        pool.awaitTermination( 100, TimeUnit.DAYS );
    }

    private static void importNodes( int numRunners, Input input, BatchingNeoStores stores, IdMapper idMapper )
            throws InterruptedException
    {
        importData( numRunners, input.nodes().iterator(), () ->
            new NodeVisitor( stores.getNeoStores(),
                    stores.getPropertyKeyRepository(), stores.getLabelRepository(), idMapper ) );
    }

    private static void importRelationships( int numRunners, Input input, BatchingNeoStores stores,
            IdMapper idMapper ) throws InterruptedException
    {
        importData( numRunners, input.relationships().iterator(), () ->
                new RelationshipVisitor( stores.getNeoStores(),
                        stores.getPropertyKeyRepository(), stores.getRelationshipTypeRepository(), idMapper ) );
    }
}
