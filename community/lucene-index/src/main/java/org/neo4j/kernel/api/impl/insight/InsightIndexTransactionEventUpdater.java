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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.properties.PropertyKeyValue;
import org.neo4j.values.storable.Values;

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
        return null;
    }

    @Override
    public void afterCommit( TransactionData data, Object state )
    {
        writeNodeData( data.assignedNodeProperties() );
    }

    private void writeNodeData( Iterable<PropertyEntry<Node>> propertyEntries )
    {
        for ( PropertyEntry<Node> propertyEntry : propertyEntries )
        {
//            propertyEntry.previouslyCommitedValue()
            Document document = LuceneInsightDocumentStructure
                    .documentRepresentingProperties( propertyEntry.entity().getId(),
                            //TODO THIS IS BAD
                            new PropertyKeyValue( Integer.parseInt( "1" ),
                                    Values.of( propertyEntry.value() ) ) );
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

    @Override
    public void afterRollback( TransactionData data, Object state )
    {

    }
}
