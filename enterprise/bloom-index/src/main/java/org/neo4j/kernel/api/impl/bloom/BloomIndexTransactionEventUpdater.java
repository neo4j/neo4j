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
package org.neo4j.kernel.api.impl.bloom;

import org.apache.lucene.document.Document;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

public class BloomIndexTransactionEventUpdater implements TransactionEventHandler<Object>
{
    private final WritableDatabaseBloomIndex nodeIndex;
    private final WritableDatabaseBloomIndex relationshipIndex;
    private final String[] properties;

    BloomIndexTransactionEventUpdater( WritableDatabaseBloomIndex nodeIndex, WritableDatabaseBloomIndex relationshipIndex, String[] properties )
    {
        this.nodeIndex = nodeIndex;
        this.relationshipIndex = relationshipIndex;
        this.properties = properties;
    }

    @Override
    public Object beforeCommit( TransactionData data ) throws Exception
    {
        Map<Long,Map<String,Object>> nodeMap = new HashMap<Long,Map<String,Object>>();
        data.removedNodeProperties().forEach( propertyEntry -> {
            try
            {
                nodeMap.put( propertyEntry.entity().getId(), propertyEntry.entity().getProperties( properties ) );
            }
            catch ( NotFoundException e )
            {
                //This means that the node was deleted.
            }
        } );
        data.assignedNodeProperties().forEach( propertyEntry -> nodeMap.put( propertyEntry.entity().getId(), propertyEntry.entity().getAllProperties() ) );

        Map<Long,Map<String,Object>> relationshipMap = new HashMap<Long,Map<String,Object>>();

        data.removedRelationshipProperties().forEach( propertyEntry -> {
            try
            {
                relationshipMap.put( propertyEntry.entity().getId(), propertyEntry.entity().getProperties( properties ) );
            }
            catch ( NotFoundException e )
            {
                //This means that the relationship was deleted.
            }
        } );
        data.assignedRelationshipProperties().forEach(
                propertyEntry -> relationshipMap.put( propertyEntry.entity().getId(), propertyEntry.entity().getProperties( properties ) ) );
        return new Map[]{nodeMap, relationshipMap};
    }

    @Override
    public void afterCommit( TransactionData data, Object state )
    {
        //update node index
        Map<Long,Map<String,Object>> nodeMap = ((Map<Long,Map<String,Object>>[]) state)[0];
        updatePropertyData( data.removedNodeProperties(), nodeMap, nodeIndex );
        updatePropertyData( data.assignedNodeProperties(), nodeMap, nodeIndex );
        deleteIndexData( data.deletedNodes(), nodeIndex );
        //update relationship index
        Map<Long,Map<String,Object>> relationshipMap = ((Map<Long,Map<String,Object>>[]) state)[1];
        updatePropertyData( data.removedNodeProperties(), relationshipMap, relationshipIndex );
        updatePropertyData( data.assignedNodeProperties(), relationshipMap, relationshipIndex );
        deleteIndexData( data.deletedNodes(), nodeIndex );
    }

    private void updatePropertyData( Iterable<PropertyEntry<Node>> propertyEntries, Map<Long,Map<String,Object>> state, WritableDatabaseBloomIndex nodeIndex )
    {
        for ( PropertyEntry<Node> propertyEntry : propertyEntries )
        {
            long nodeId = propertyEntry.entity().getId();
            Map<String,Object> allProperties = state.get( nodeId );
            if ( allProperties == null )
            {
                return;
            }

            Document document = BloomDocumentStructure.documentRepresentingProperties( nodeId, allProperties );
            try
            {
                nodeIndex.getIndexWriter().updateDocument( BloomDocumentStructure.newTermForChangeOrRemove( nodeId ), document );
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }
        }
        try
        {
            nodeIndex.maybeRefreshBlocking();
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }

    private void deleteIndexData( Iterable<Node> nodes, WritableDatabaseBloomIndex nodeIndex )
    {
        for ( Node node : nodes )
        {
            try
            {
                nodeIndex.getIndexWriter().deleteDocuments( BloomDocumentStructure.newTermForChangeOrRemove( node.getId() ) );
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }
        }
        try
        {
            nodeIndex.maybeRefreshBlocking();
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
