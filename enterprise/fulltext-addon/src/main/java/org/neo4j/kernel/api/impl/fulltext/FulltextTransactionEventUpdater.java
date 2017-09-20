/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.fulltext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.logging.Log;

class FulltextTransactionEventUpdater implements TransactionEventHandler<Object>
{
    private final FulltextProvider fulltextProvider;
    private final Log log;
    private final FulltextUpdateApplier applier;

    FulltextTransactionEventUpdater( FulltextProvider fulltextProvider, Log log,
                                     FulltextUpdateApplier applier )
    {
        this.fulltextProvider = fulltextProvider;
        this.log = log;
        this.applier = applier;
    }

    @Override
    public Object beforeCommit( TransactionData data ) throws Exception
    {
        String[] nodeProperties = fulltextProvider.getNodeProperties();
        Map<Long,Map<String,Object>> nodeMap = new HashMap<Long,Map<String,Object>>();
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
                propertyEntry -> nodeMap.put( propertyEntry.entity().getId(), propertyEntry.entity().getProperties( nodeProperties ) ) );

        String[] relationshipProperties = fulltextProvider.getRelationshipProperties();
        Map<Long,Map<String,Object>> relationshipMap = new HashMap<Long,Map<String,Object>>();
        data.removedRelationshipProperties().forEach( propertyEntry ->
        {
            try
            {
                relationshipMap.put( propertyEntry.entity().getId(), propertyEntry.entity().getProperties( relationshipProperties ) );
            }
            catch ( NotFoundException e )
            {
                //This means that the relationship was deleted.
            }
        } );
        data.assignedRelationshipProperties().forEach(
                propertyEntry -> relationshipMap.put( propertyEntry.entity().getId(), propertyEntry.entity().getProperties( relationshipProperties ) ) );
        return new Map[]{nodeMap, relationshipMap};
    }

    @Override
    public void afterCommit( TransactionData data, Object state )
    {
        try
        {
            List<AsyncFulltextIndexOperation> completions = new ArrayList<>();
            Map<Long,Map<String,Object>> nodeMap = ((Map<Long,Map<String,Object>>[]) state)[0];
            Map<Long,Map<String,Object>> relationshipMap = ((Map<Long,Map<String,Object>>[]) state)[1];

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

            for ( AsyncFulltextIndexOperation completion : completions )
            {
                completion.awaitCompletion();
            }
        }
        catch ( IOException e )
        {
            log.error( "Unable to update fulltext index", e );
        }
    }

    @Override
    public void afterRollback( TransactionData data, Object state )
    {

    }
}
