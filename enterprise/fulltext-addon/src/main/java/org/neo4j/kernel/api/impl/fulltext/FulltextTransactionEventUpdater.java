/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.api.impl.fulltext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

class FulltextTransactionEventUpdater implements TransactionEventHandler<FulltextTransactionEventUpdater.FulltextTransactionContext>
{
    private final FulltextProviderImpl fulltextProvider;
    private final FulltextUpdateApplier applier;

    FulltextTransactionEventUpdater( FulltextProviderImpl fulltextProvider, FulltextUpdateApplier applier )
    {
        this.fulltextProvider = fulltextProvider;
        this.applier = applier;
    }

    @Override
    public FulltextTransactionContext beforeCommit( TransactionData data )
    {
        PrimitiveLongObjectMap<Map<String,Object>> nodeMap = Primitive.longObjectMap();
        PrimitiveLongObjectMap<Map<String,Object>> relationshipMap = Primitive.longObjectMap();
        Lock lock = fulltextProvider.readLockIndexConfiguration();
        FulltextTransactionContext fulltextTransactionContext = new FulltextTransactionContext( nodeMap, relationshipMap, lock );

        String[] nodeProperties = fulltextProvider.getNodeProperties();
        data.removedNodeProperties().forEach( propertyEntry ->
        {
            try
            {
                nodeMap.put( propertyEntry.entity().getId(), propertyEntry.entity().getProperties( nodeProperties ) );
            }
            catch ( NotFoundException e )
            {
                //This means that the node was deleted.
            }
        } );
        data.assignedNodeProperties().forEach(
                propertyEntry -> nodeMap.put( propertyEntry.entity().getId(),
                        propertyEntry.entity().getProperties( nodeProperties ) ) );

        String[] relationshipProperties = fulltextProvider.getRelationshipProperties();
        data.removedRelationshipProperties().forEach( propertyEntry ->
        {
            try
            {
                relationshipMap.put( propertyEntry.entity().getId(),
                        propertyEntry.entity().getProperties( relationshipProperties ) );
            }
            catch ( NotFoundException e )
            {
                //This means that the relationship was deleted.
            }
        } );
        data.assignedRelationshipProperties().forEach(
                propertyEntry -> relationshipMap.put( propertyEntry.entity().getId(),
                        propertyEntry.entity().getProperties( relationshipProperties ) ) );
        return fulltextTransactionContext;
    }

    @Override
    public void afterCommit( TransactionData data, FulltextTransactionContext state )
    {
        RuntimeException applyException = null;
        List<AsyncFulltextIndexOperation> completions = new ArrayList<>();
        try
        {
            try
            {
                PrimitiveLongObjectMap<Map<String,Object>> nodeMap = state.getNodeMap();
                PrimitiveLongObjectMap<Map<String,Object>> relationshipMap = state.getRelationshipMap();

                //update node indices
                for ( WritableFulltext nodeIndex : fulltextProvider.writableNodeIndices() )
                {
                    completions.add( applier.removePropertyData( data.removedNodeProperties(), nodeMap, nodeIndex ) );
                    completions.add( applier.updatePropertyData( nodeMap, nodeIndex ) );
                }

                //update relationship indices
                for ( WritableFulltext relationshipIndex : fulltextProvider.writableRelationshipIndices() )
                {
                    completions.add( applier.removePropertyData( data.removedRelationshipProperties(), relationshipMap, relationshipIndex ) );
                    completions.add( applier.updatePropertyData( relationshipMap, relationshipIndex ) );
                }
            }
            catch ( IOException e )
            {
                applyException = new RuntimeException( "Failed to submit all index updates.", e );
            }

            for ( AsyncFulltextIndexOperation completion : completions )
            {
                try
                {
                    completion.awaitCompletion();
                }
                catch ( ExecutionException e )
                {
                    if ( applyException == null )
                    {
                        applyException = new RuntimeException( "Failed to update fulltext index. See suppressed exceptions for details." );
                    }
                    applyException.addSuppressed( e );
                }
            }
            if ( applyException != null )
            {
                throw applyException;
            }
        }
        finally
        {
            state.release();
        }
    }

    @Override
    public void afterRollback( TransactionData data, FulltextTransactionContext state )
    {
        state.release();
    }

    public static class FulltextTransactionContext
    {
        private final PrimitiveLongObjectMap<Map<String,Object>> nodeMap;
        private final PrimitiveLongObjectMap<Map<String,Object>> relationshipMap;
        private final Lock lock;

        private FulltextTransactionContext( PrimitiveLongObjectMap<Map<String,Object>> nodeMap,
                PrimitiveLongObjectMap<Map<String,Object>> relationshipMap, Lock lock )
        {
            this.nodeMap = nodeMap;
            this.relationshipMap = relationshipMap;
            this.lock = lock;
        }

        public PrimitiveLongObjectMap<Map<String,Object>> getRelationshipMap()
        {
            return relationshipMap;
        }

        public PrimitiveLongObjectMap<Map<String,Object>> getNodeMap()
        {
            return nodeMap;
        }

        public void release()
        {
            lock.unlock();
        }
    }
}
