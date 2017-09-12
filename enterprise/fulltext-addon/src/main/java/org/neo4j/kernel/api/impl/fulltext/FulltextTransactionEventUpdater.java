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

import org.apache.lucene.document.Document;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.logging.Log;

class FulltextTransactionEventUpdater implements TransactionEventHandler<Object>
{

    private final FulltextProvider fulltextProvider;
    private final Log log;

    FulltextTransactionEventUpdater( FulltextProvider fulltextProvider, Log log )
    {
        this.fulltextProvider = fulltextProvider;
        this.log = log;
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
        //update node indices
        try
        {
            for ( WritableFulltext nodeIndex : fulltextProvider.writableNodeIndices() )
            {
                Map<Long,Map<String,Object>> nodeMap = ((Map<Long,Map<String,Object>>[]) state)[0];
                removePropertyData( data.removedNodeProperties(), nodeMap, nodeIndex );
                updatePropertyData( nodeMap, nodeIndex );
                refreshIndex( nodeIndex );
            }
            //update relationship indices
            for ( WritableFulltext relationshipIndex : fulltextProvider.writableRelationshipIndices() )
            {
                Map<Long,Map<String,Object>> relationshipMap = ((Map<Long,Map<String,Object>>[]) state)[1];
                removePropertyData( data.removedRelationshipProperties(), relationshipMap, relationshipIndex );
                updatePropertyData( relationshipMap, relationshipIndex );
                refreshIndex( relationshipIndex );
            }
        }
        catch ( IOException e )
        {
            log.error( "Unable to update fulltext index", e );
        }
    }

    private <E extends Entity> void updatePropertyData( Map<Long,Map<String,Object>> state, WritableFulltext index ) throws IOException
    {
        for ( Map.Entry<Long,Map<String,Object>> stateEntry : state.entrySet() )
        {
            Set<String> indexedProperties = index.properties();
            if ( !Collections.disjoint( indexedProperties, stateEntry.getValue().keySet() ) )
            {
                long entityId = stateEntry.getKey();
                Map<String,Object> allProperties =
                        stateEntry.getValue().entrySet().stream().filter( entry -> indexedProperties.contains( entry.getKey() ) ).collect(
                                Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );
                if ( !allProperties.isEmpty() )
                {
                    Document document = LuceneFulltextDocumentStructure.documentRepresentingProperties( entityId, allProperties );
                    index.getIndexWriter().updateDocument( LuceneFulltextDocumentStructure.newTermForChangeOrRemove( entityId ), document );
                }
            }
        }
    }

    private <E extends Entity> void removePropertyData( Iterable<PropertyEntry<E>> propertyEntries, Map<Long,Map<String,Object>> state,
            WritableFulltext index ) throws IOException
    {
        for ( PropertyEntry<E> propertyEntry : propertyEntries )
        {
            if ( index.properties().contains( propertyEntry.key() ) )
            {
                long entityId = propertyEntry.entity().getId();
                Map<String,Object> allProperties = state.get( entityId );
                if ( allProperties == null || allProperties.isEmpty() )
                {
                    index.getIndexWriter().deleteDocuments( LuceneFulltextDocumentStructure.newTermForChangeOrRemove( entityId ) );
                }
            }
        }
    }

    private void refreshIndex( WritableFulltext index )
    {
        try
        {
            index.maybeRefreshBlocking();
        }
        catch ( IOException e )
        {
            log.error( "Failed to refresh fulltext after updates", e );
        }
    }

    @Override
    public void afterRollback( TransactionData data, Object state )
    {

    }
}
