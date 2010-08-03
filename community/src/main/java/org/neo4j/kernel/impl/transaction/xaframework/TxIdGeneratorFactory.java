package org.neo4j.kernel.impl.transaction.xaframework;

import javax.transaction.TransactionManager;

public interface TxIdGeneratorFactory
{
    public static final TxIdGeneratorFactory DEFAULT = new TxIdGeneratorFactory()
    {
        public TxIdGenerator create( final TransactionManager txManager )
        {
            return TxIdGenerator.DEFAULT;
        }
    }; 
    
    TxIdGenerator create( TransactionManager txManager );
}
