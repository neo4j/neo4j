package org.neo4j.kernel.impl.core;

public interface LastCommittedTxIdSetter
{
    void setLastCommittedTxId( long txId );
}
