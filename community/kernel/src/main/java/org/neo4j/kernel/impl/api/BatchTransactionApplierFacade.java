package org.neo4j.kernel.impl.api;

import java.io.IOException;

import org.neo4j.kernel.impl.locking.LockGroup;

/**
 * This class wraps several {@link BatchTransactionApplier}s which will do their work sequentially. See also {@link
 * TransactionApplierFacade} which is used to wrap the {@link #startTx(TransactionToApply)} and {@link
 * #startTx(TransactionToApply, LockGroup)} methods.
 */
public class BatchTransactionApplierFacade implements BatchTransactionApplier
{

    private final BatchTransactionApplier[] appliers;

    public BatchTransactionApplierFacade( BatchTransactionApplier... appliers )
    {
        this.appliers = appliers;
    }

    @Override
    public TransactionApplier startTx( TransactionToApply transaction ) throws IOException
    {
        TransactionApplier[] txAppliers = new TransactionApplier[appliers.length];
        for ( int i = 0; i < appliers.length; i++ )
        {
            txAppliers[i] = appliers[i].startTx( transaction );
        }
        return new TransactionApplierFacade( txAppliers );
    }

    @Override
    public TransactionApplier startTx( TransactionToApply transaction, LockGroup lockGroup ) throws IOException
    {
        TransactionApplier[] txAppliers = new TransactionApplier[appliers.length];
        for ( int i = 0; i < appliers.length; i++ )
        {
            txAppliers[i] = appliers[i].startTx( transaction, lockGroup );
        }
        return new TransactionApplierFacade( txAppliers );
    }

    @Override
    public void close() throws Exception
    {
        // Not sure why it is necessary to close them in reverse order
        for ( int i = appliers.length; i-- > 0; )
        {
            appliers[i].close();
        }
    }
}
