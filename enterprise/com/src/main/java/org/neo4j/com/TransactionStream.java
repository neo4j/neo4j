package org.neo4j.com;

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.transaction.xaframework.CommittedTransactionRepresentation;

public interface TransactionStream
{
    void accept( Visitor<CommittedTransactionRepresentation, IOException> visitor ) throws IOException;

    public static final TransactionStream EMPTY = new TransactionStream()
    {
        @Override
        public void accept( Visitor<CommittedTransactionRepresentation, IOException> visitor ) throws IOException
        {
        }
    };
}
