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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;

public class DeeshuImporter
{
//    public static void main( String[] args ) throws InterruptedException
//    {
//        int numRunners = Runtime.getRuntime().availableProcessors();
//        Configuration configuration = Configuration.COMMAS;
//        File database = new File( "K:/graph.db" );
//        try ( DefaultFileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();
//              Collector collector = new BadCollector( System.out, 0, 0 ); )
//        {
//            fileSystemAbstraction.deleteRecursively( database );
//            DataFactory nodeData = data( NO_DECORATOR, defaultCharset(), new File( "K:/csv/nodes.csv" ) );
//            DataFactory relationshipData = data( NO_DECORATOR, defaultCharset(),
//                    new File( "K:/csv/relationships2.csv" ) );
//            IdType idType = IdType.STRING;
//            Input input = new CsvInput(
//                    // nodes
//                    datas( nodeData ), defaultFormatNodeFileHeader(),
//                    // relationships
//                    datas( relationshipData ), defaultFormatRelationshipFileHeader(),
//                    idType, configuration, collector );
//
//            Config config = Config.defaults();
//            RecordFormats format = RecordFormatSelector.selectForConfig( config, NullLogProvider.getInstance() );
//            try ( BatchingNeoStores stores = BatchingNeoStores.batchingNeoStores(
//                    fileSystemAbstraction, database, format, DEFAULT, NullLogService.getInstance(), EMPTY, config ) )
//            {
//                IdMapper idMapper = idType.idMapper();
//                long time = currentTimeMillis();
//                stores.startFlushingPageCache();
//                importNodes( numRunners, input, stores, idMapper );
//                stores.stopFlushingPageCache();
//                idMapper.prepare( new NodeInputIdPropertyLookup( stores.getNodeStore(), stores.getPropertyStore() ),
//                        collector, ProgressListener.NONE );
//                stores.startFlushingPageCache();
//                importRelationships( numRunners, input, stores, idMapper );
//                stores.stopFlushingPageCache();
//                time = currentTimeMillis() - time;
//                System.out.println( "Imported in " + duration( time ) );
//            }
//        }
//        catch ( IOException e )
//        {
//            e.printStackTrace();
//        }
//    }

    private static void importData( int numRunners, InputIterator data, Supplier<EntityVisitor> visitors )
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

    public static void importNodes( int numRunners, Input input, BatchingNeoStores stores, IdMapper idMapper,
            NodeRelationshipCache nodeRelationshipCache )
            throws InterruptedException
    {
        importData( numRunners, input.nodes(), () ->
            new NodeVisitor( stores.getNeoStores(),
                    stores.getPropertyKeyRepository(), stores.getLabelRepository(), idMapper ) );
        nodeRelationshipCache.setHighNodeId( stores.getNodeStore().getHighId() );
    }

    public static RelationshipTypeDistribution importRelationships( int numRunners, Input input,
            BatchingNeoStores stores, IdMapper idMapper, NodeRelationshipCache nodeRelationshipCache )
                    throws InterruptedException
    {
        RelationshipTypeDistribution typeDistribution = new RelationshipTypeDistribution();
        importData( numRunners, input.relationships(), () ->
                new RelationshipVisitor( stores.getNeoStores(),
                        stores.getPropertyKeyRepository(), stores.getRelationshipTypeRepository(), idMapper,
                        nodeRelationshipCache, typeDistribution ) );
        return typeDistribution;
    }
}
