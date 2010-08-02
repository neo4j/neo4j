package org.neo4j.kernel;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.Transaction;

public interface TopLevelTransactionFactory
{
    public static final TopLevelTransactionFactory DEFAULT = new TopLevelTransactionFactory()
    {
        private Transaction placeboTx;
        
        public Transaction beginTx( TransactionManager txManager )
        {
            return new TopLevelTransaction( txManager );
        }
    
        public Transaction getPlaceboTx( TransactionManager txManager )
        {
            if ( placeboTx == null )
            {
                placeboTx = new PlaceboTransaction( txManager );
            }
            return placeboTx;
        }
    };

    Transaction beginTx( TransactionManager txManager );
    
    Transaction getPlaceboTx( TransactionManager txManager );
}
