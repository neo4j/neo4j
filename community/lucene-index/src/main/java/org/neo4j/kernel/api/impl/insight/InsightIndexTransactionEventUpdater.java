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
package org.neo4j.kernel.api.impl.insight;

import org.apache.lucene.document.Document;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

public class InsightIndexTransactionEventUpdater implements TransactionEventHandler<Object>
{
    private WritableDatabaseInsightIndex writableDatabaseInsightIndex;

    InsightIndexTransactionEventUpdater( WritableDatabaseInsightIndex writableDatabaseInsightIndex )
    {
        this.writableDatabaseInsightIndex = writableDatabaseInsightIndex;
    }

    @Override
    public Object beforeCommit( TransactionData data ) throws Exception
    {
        Map<Long,Map<String,Object>> map = new HashMap<Long,Map<String,Object>>();
        data.createdNodes().forEach( node -> map.put( node.getId(), node.getAllProperties() ) );
        return map;
    }

    @Override
    public void afterCommit( TransactionData data, Object state )
    {
        writeNodeData( data.createdNodes(), (Map<Long,Map<String,Object>>) state );
        deleteNodeData( data.deletedNodes() );
    }

    private void writeNodeData( Iterable<Node> propertyEntries, Map<Long,Map<String,Object>> state )
    {
        for ( Node node : propertyEntries )
        {

            Map<String,Object> allProperties = state.get( node.getId() );

            Document document = LuceneInsightDocumentStructure.documentRepresentingProperties( node.getId(), allProperties );
            try
            {
                writableDatabaseInsightIndex.getIndexWriter().addDocument( document );
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }
        }
        try
        {
            writableDatabaseInsightIndex.maybeRefreshBlocking();
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }

    private void deleteNodeData( Iterable<Node> nodes )
    {
        for ( Node node : nodes )
        {
            try
            {
                writableDatabaseInsightIndex.getIndexWriter().deleteDocuments( LuceneInsightDocumentStructure.newTermForChangeOrRemove( node.getId() ) );
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }
        }
        try
        {
            writableDatabaseInsightIndex.maybeRefreshBlocking();
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
