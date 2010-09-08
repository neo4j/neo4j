package org.neo4j.kernel.ha.comm;

import java.nio.channels.ReadableByteChannel;

public final class TransactionEntry
{
    final String resource;
    final long txId;
    final ReadableByteChannel data;

    TransactionEntry( String resource, long txId, ReadableByteChannel data )
    {
        this.resource = resource;
        this.txId = txId;
        this.data = data;
    }
}
