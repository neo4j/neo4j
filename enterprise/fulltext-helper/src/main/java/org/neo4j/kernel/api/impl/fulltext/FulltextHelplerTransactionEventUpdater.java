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
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

public class FulltextHelplerTransactionEventUpdater implements TransactionEventHandler<Object>
{

    private FulltextHelperProvider fulltextHelperProvider;

    public FulltextHelplerTransactionEventUpdater( FulltextHelperProvider fulltextHelperProvider )
    {
        this.fulltextHelperProvider = fulltextHelperProvider;
    }

    @Override
    public Object beforeCommit( TransactionData data ) throws Exception
    {
        String[] nodeProperties = fulltextHelperProvider.getNodeProperties();
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

        String[] relationshipProperties = fulltextHelperProvider.getRelationshipProperties();
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
        Map<Long,Map<String,Object>> nodeMap = ((Map<Long,Map<String,Object>>[]) state)[0];
        for ( WritableDatabaseBloomIndex nodeIndex : fulltextHelperProvider.nodeIndices() )
        {
            updatePropertyData( data.removedNodeProperties(), nodeMap, nodeIndex );
            updatePropertyData( data.assignedNodeProperties(), nodeMap, nodeIndex );
            deleteIndexData( data.deletedNodes(), nodeIndex );
        }
        //update relationship index
        for ( WritableDatabaseBloomIndex relationshipIndex : fulltextHelperProvider.relationshipIndices() )
        {
            Map<Long,Map<String,Object>> relationshipMap = ((Map<Long,Map<String,Object>>[]) state)[1];
            updatePropertyData( data.removedRelationshipProperties(), relationshipMap, relationshipIndex );
            updatePropertyData( data.assignedRelationshipProperties(), relationshipMap, relationshipIndex );
            deleteIndexData( data.deletedRelationships(), relationshipIndex );
        }
    }

    private <E extends Entity> void updatePropertyData( Iterable<PropertyEntry<E>> propertyEntries, Map<Long,Map<String,Object>> state,
            WritableDatabaseBloomIndex index )
    {
        for ( PropertyEntry<E> propertyEntry : propertyEntries )
        {
            if ( index.properites().contains( propertyEntry.key() ) )
            {
                long entityId = propertyEntry.entity().getId();
                Map<String,Object> allProperties = state.get( entityId );
                if ( allProperties == null )
                {
                    try
                    {
                        index.getIndexWriter().deleteDocuments( BloomDocumentStructure.newTermForChangeOrRemove( entityId ) );
                    }
                    catch ( IOException e )
                    {
                        e.printStackTrace();
                    }
                    continue;
                }

                Document document = BloomDocumentStructure.documentRepresentingProperties( entityId, allProperties );
                try
                {
                    index.getIndexWriter().updateDocument( BloomDocumentStructure.newTermForChangeOrRemove( entityId ), document );
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        }
        try
        {
            index.maybeRefreshBlocking();
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }

    private <E extends Entity> void deleteIndexData( Iterable<E> entities, WritableDatabaseBloomIndex index )
    {
        for ( E entity : entities )
        {
            try
            {
                index.getIndexWriter().deleteDocuments( BloomDocumentStructure.newTermForChangeOrRemove( entity.getId() ) );
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }
        }
        try
        {
            index.maybeRefreshBlocking();
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }

    @Override
    public void afterRollback( TransactionData data, Object state )
    {

    }
}
