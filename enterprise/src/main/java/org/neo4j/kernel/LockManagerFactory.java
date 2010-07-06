package org.neo4j.kernel;

import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TxModule;

public interface LockManagerFactory
{
    LockManager create( TxModule txModule );
}
