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
    private PartitionedInsightIndexWriter indexWriter;

    InsightIndexTransactionEventUpdater( PartitionedInsightIndexWriter indexWriter )
    {
        this.indexWriter = indexWriter;
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
                indexWriter.addDocument( document );
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void afterRollback( TransactionData data, Object state )
    {

    }
}
