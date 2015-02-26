/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

package org.neo4j.kernel.impl.store.counts;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.api.CommandApplierFacade;
import org.neo4j.kernel.impl.api.CountsRecordState;
import org.neo4j.kernel.impl.api.CountsStoreApplier;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.pagecache.StandalonePageCache;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.counts.keys.IndexSampleKey;
import org.neo4j.kernel.impl.store.counts.keys.IndexStatisticsKey;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.NeoCommandHandler;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.kernel.impl.store.StoreFactory.buildTypeDescriptorAndVersion;

public class RecountFromTransactions
{
    public static void main( String... args ) throws Exception
    {
        if ( args.length != 1 )
        {
            System.err.println( "Expecting exactly one argument describing the path to the store" );
            System.exit( 1 );
        }

        final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        File path = new File( args[0] );

        if ( !fs.isDirectory( path ) )
        {
            System.err.println( args[0] + " is not a directory." );
            System.exit( 1 );
        }

        TransactionStream transactions = new TransactionStream( fs, path );
        if ( transactions.firstTransactionId() != 1 || !transactions.isContiguous() )
        {
            System.err.println( "Transaction stream is not contiguous:" + transactions.rangeString( "\n\t" ) );
            System.exit( 1 );
        }

        File nodeStore = new File( path, "recount-from-tx" );
        try ( StandalonePageCache pages = createPageCache( fs, "recount-from-tx" ) )
        {
            StoreFactory factory = new StoreFactory( fs, path, pages, StringLogger.DEV_NULL, new Monitors() );

            CountsRecordState counts = rebuildCounts( transactions, nodeStore, factory );

            DumpCountsStore dump = new DumpCountsStore( System.out, factory );
            System.out.println( "Expected counts:" );
            counts.accept( dump );

            verifyCounts( factory, counts, dump );
        }
        finally
        {
            deleteStore( fs, nodeStore );
            deleteStore( fs, nodeLabelStore( nodeStore ) );
        }
    }

    private static CountsRecordState rebuildCounts( TransactionStream transactions, File nodeStore,
                                                    StoreFactory factory ) throws IOException
    {
        CountsRecordState counts = new CountsRecordState();
        try ( NodeStore nodes = createNodeStore( factory, nodeStore ) )
        {
            try ( IOCursor<CommittedTransactionRepresentation> cursor = transactions.cursor() )
            {
                while ( cursor.next() )
                {
                    try ( CommandApplierFacade visitor = new CommandApplierFacade(
                            new NodeStoreApplier( nodes ), new CountsStoreApplier( counts, nodes ) ) )
                    {
                        cursor.get().accept( visitor );
                    }
                }
            }
        }
        return counts;
    }

    private static void verifyCounts( StoreFactory factory, CountsRecordState counts, CountsVisitor dump )
    {
        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker tracker = life.add( factory.newCountsStore() );
            tracker.accept( dump );
            List<CountsRecordState.Difference> differences = counts.verify( tracker );
            boolean equal = true;
            for ( CountsRecordState.Difference difference : differences )
            {
                if ( difference.key() instanceof IndexSampleKey ||
                     difference.key() instanceof IndexStatisticsKey )
                {
                    continue;
                }
                if ( equal )
                {
                    System.out.println( "Difference in counts:" );
                    equal = false;
                }
                System.out.println( "\t" + difference );
            }
            if ( equal )
            {
                System.out.println( "Counts store has expected data." );
            }
        }
    }

    private static void deleteStore( FileSystemAbstraction fs, File storeFile )
    {
        fs.deleteFile( storeFile );
        fs.deleteFile( new File( storeFile.getPath() + ".id" ) );
    }

    private static NodeStore createNodeStore( StoreFactory factory, File nodeStore )
    {
        int labelStoreBlockSize = 60;
        factory.createEmptyDynamicStore(
                nodeLabelStore( nodeStore ), labelStoreBlockSize, DynamicArrayStore.VERSION, IdType.NODE_LABELS );
        factory.createEmptyStore( nodeStore, buildTypeDescriptorAndVersion( NodeStore.TYPE_DESCRIPTOR ) );
        return factory.newNodeStore( nodeStore );
    }

    private static File nodeLabelStore( File nodeStore )
    {
        return new File( nodeStore.getPath() + StoreFactory.LABELS_PART );
    }

    private static class NodeStoreApplier extends NeoCommandHandler.Adapter
    {
        private final NodeStore nodes;

        NodeStoreApplier( NodeStore nodes )
        {
            this.nodes = nodes;
        }

        @Override
        public boolean visitNodeCommand( Command.NodeCommand command ) throws IOException
        {
            NodeRecord state = command.getAfter();
            if ( state.inUse() )
            {
                nodes.updateRecord( state );
            }
            Collection<DynamicRecord> records = state.getDynamicLabelRecords();
            List<DynamicRecord> updated = new ArrayList<>( records.size() );
            for ( DynamicRecord record : records )
            {
                if ( record.inUse() )
                {
                    updated.add( record );
                }
            }
            nodes.updateDynamicLabelRecords( updated );
            return false;
        }
    }
}
