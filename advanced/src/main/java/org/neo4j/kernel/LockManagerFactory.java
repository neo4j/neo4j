package org.neo4j.kernel;

import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TxModule;

public interface LockManagerFactory
{
    public static final LockManagerFactory DEFAULT = new LockManagerFactory()
    {
        public LockManager create( TxModule txModule )
        {
            return new LockManager( txModule.getTxManager() );
        }
    };
    
    LockManager create( TxModule txModule );
}
