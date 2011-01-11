package org.neo4j.kernel.impl.transaction;

import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;

public final class DefaultTransactionManagerProvider extends TransactionManagerProvider
{
    public DefaultTransactionManagerProvider()
    {
        super( "native" );
    }

    @Override
    protected AbstractTransactionManager loadTransactionManager( String txLogDir,
            KernelPanicEventGenerator kpe, TxFinishHook rollbackHook )
    {
        return new TxManager( txLogDir, kpe, rollbackHook );
    }
}
