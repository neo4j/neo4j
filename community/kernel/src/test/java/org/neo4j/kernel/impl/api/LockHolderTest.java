package org.neo4j.kernel.impl.api;

import javax.transaction.Transaction;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.neo4j.kernel.impl.core.SchemaLock;
import org.neo4j.kernel.impl.transaction.LockManager;

public class LockHolderTest
{
    @Test
    public void shouldAcquireSchemaReadLock()
    {
        // GIVEN
        LockManager mgr = Mockito.mock( LockManager.class );
        Transaction tx = Mockito.mock( Transaction.class );
        LockHolder holder = new LockHolder( mgr, tx );

        // WHEN
        holder.acquireSchemaReadLock();


        // THEN
        Mockito.verify( mgr ).getReadLock( Matchers.any( SchemaLock.class ), Matchers.eq( tx ) );
    }

    @Test
    public void shouldAcquireSchemaWriteLock()
    {
        // GIVEN
        LockManager mgr = Mockito.mock( LockManager.class );
        Transaction tx = Mockito.mock( Transaction.class );
        LockHolder holder = new LockHolder( mgr, tx );

        // WHEN
        holder.acquireSchemaWriteLock();


        // THEN
        Mockito.verify( mgr ).getWriteLock( Matchers.any( SchemaLock.class ), Matchers.eq( tx ) );
    }

    @Test
    public void shouldReleaseSchemaReadLockOnRelease()
    {
        // GIVEN
        LockManager mgr = Mockito.mock( LockManager.class );
        Transaction tx = Mockito.mock( Transaction.class );
        LockHolder holder = new LockHolder( mgr, tx );

        // WHEN
        holder.acquireSchemaReadLock();
        holder.releaseLocks();


        // THEN
        Mockito.verify( mgr ).releaseReadLock( Matchers.any( SchemaLock.class ), Matchers.eq( tx ) );
    }

    @Test
    public void shouldReleaseSchemaWriteLockOnRelease()
    {
        // GIVEN
        LockManager mgr = Mockito.mock( LockManager.class );
        Transaction tx = Mockito.mock( Transaction.class );
        LockHolder holder = new LockHolder( mgr, tx );

        // WHEN
        holder.acquireSchemaWriteLock();
        holder.releaseLocks();


        // THEN
        Mockito.verify( mgr ).releaseWriteLock( Matchers.any( SchemaLock.class ), Matchers.eq( tx ) );
    }



}
