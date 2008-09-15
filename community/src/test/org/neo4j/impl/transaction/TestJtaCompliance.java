/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.transaction;

import javax.transaction.NotSupportedException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.neo4j.impl.AbstractNeoTestCase;
import org.neo4j.impl.transaction.xaframework.XaConnection;
import org.neo4j.impl.transaction.xaframework.XaDataSource;

public class TestJtaCompliance extends AbstractNeoTestCase
{
    // the TransactionManager to use when testing for JTA compliance
    private TransactionManager tm;
    private XaDataSourceManager xaDsMgr;

    public TestJtaCompliance( String name )
    {
        super( name );
    }

    public static Test suite()
    {
        return new TestSuite( TestJtaCompliance.class );
    }

    public void setUp()
    {
        super.setUp();
        getTransaction().finish();
        TxModule txModule = getEmbeddedNeo().getConfig().getTxModule();
        tm = txModule.getTxManager();
        xaDsMgr = txModule.getXaDataSourceManager();
        java.util.Map<String,FakeXAResource> map1 = new java.util.HashMap<String,FakeXAResource>();
        map1.put( "xa_resource", new FakeXAResource( "XAResource1" ) );
        java.util.Map<String,FakeXAResource> map2 = new java.util.HashMap<String,FakeXAResource>();
        map2.put( "xa_resource", new FakeXAResource( "XAResource2" ) );
        try
        {
            xaDsMgr.registerDataSource( "fakeRes1",
                new DummyXaDataSource( map1 ), "0xDDDDDE".getBytes() );
            xaDsMgr.registerDataSource( "fakeRes2",
                new DummyXaDataSource( map2 ), "0xDDDDDF".getBytes() );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
        try
        {
            // make sure were not in transaction
            tm.commit();
        }
        catch ( Exception e )
        {
        }
        Transaction tx = null;
        try
        {
            tx = tm.getTransaction();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Unkown state of TM" );
        }
        if ( tx != null )
        {
            throw new RuntimeException( "We're still in transaction" );
        }
    }

    public void tearDown()
    {
        xaDsMgr.unregisterDataSource( "fakeRes1" );
        xaDsMgr.unregisterDataSource( "fakeRes2" );
        try
        {
            if ( tm.getTransaction() == null )
            {
                try
                {
                    tm.begin();
                }
                catch ( Exception e )
                {
                }
            }
        }
        catch ( SystemException e )
        {
            e.printStackTrace();
        }
        super.tearDown();
    }

    /**
     * o Tests that tm.begin() starts a global transaction and associates the
     * calling thread with that transaction. o Tests that after commit is
     * invoked transaction is completed and a repeating call to commit/rollback
     * results in an exception.
     * 
     * TODO: check if commit is restricted to the thread that started the
     * transaction, if not, do some testing.
     */
    public void testBeginCommit() throws Exception
    {
        tm.begin();
        assertTrue( tm.getTransaction() != null );
        tm.commit(); // drop current transaction
        assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );
        try
        {
            tm.rollback();
            fail( "rollback() should throw an exception -> "
                + "STATUS_NO_TRANSACTION" );
        }
        catch ( IllegalStateException e )
        {
            // good
        }
        try
        {
            tm.commit();
            fail( "commit() should throw an exception -> "
                + "STATUS_NO_TRANSACTION" );
        }
        catch ( IllegalStateException e )
        {
            // good
        }
    }

    /**
     * o Tests that after rollback is invoked the transaction is completed and a
     * repeating call to rollback/commit results in an exception.
     * 
     * TODO: check if rollback is restricted to the thread that started the
     * transaction, if not, do some testing.
     */
    public void testBeginRollback() throws Exception
    {
        tm.begin();
        assertTrue( tm.getTransaction() != null );
        tm.rollback(); // drop current transaction
        assertTrue( tm.getStatus() == Status.STATUS_NO_TRANSACTION );
        try
        {
            tm.commit();
            fail( "commit() should throw an exception -> "
                + "STATUS_NO_TRANSACTION" );
        }
        catch ( IllegalStateException e )
        {
            // good
        }
        try
        {
            tm.rollback();
            fail( "rollback() should throw an exception -> "
                + "STATUS_NO_TRANSACTION" );
        }
        catch ( IllegalStateException e )
        {
            // good
        }
    }

    /**
     * o Tests that suspend temporarily suspends the transaction associated with
     * the calling thread. o Tests that resume reinstate the transaction with
     * the calling thread. o Tests that an invalid transaction passed to resume
     * won't be associated with the calling thread. o Tests that XAResource.end
     * is invoked with TMSUSPEND when transaction is suspended. o Tests that
     * XAResource.start is invoked with TMRESUME when transaction is resumed.
     * 
     * TODO: o Test that resume throws an exception if the transaction is
     * already associated with another thread. o Test if a suspended thread may
     * be resumed by another thread.
     */
    public void testSuspendResume() throws Exception
    {
        tm.begin();
        Transaction tx = tm.getTransaction();
        FakeXAResource res = new FakeXAResource( "XAResource1" );
        tx.enlistResource( res );

        // suspend
        assertTrue( tm.suspend() == tx );
        tx.delistResource( res, XAResource.TMSUSPEND );
        MethodCall calls[] = res.getAndRemoveMethodCalls();
        assertEquals( 2, calls.length );
        assertEquals( "start", calls[0].getMethodName() );
        Object args[] = calls[0].getArgs();
        assertEquals( XAResource.TMNOFLAGS, ((Integer) args[1]).intValue() );
        assertEquals( "end", calls[1].getMethodName() );
        args = calls[1].getArgs();
        assertEquals( XAResource.TMSUSPEND, ((Integer) args[1]).intValue() );

        // resume
        tm.resume( tx );
        tx.enlistResource( res );
        calls = res.getAndRemoveMethodCalls();
        assertEquals( 1, calls.length );
        assertEquals( "start", calls[0].getMethodName() );
        args = calls[0].getArgs();
        assertEquals( XAResource.TMRESUME, ((Integer) args[1]).intValue() );
        assertTrue( tm.getTransaction() == tx );
        tx.delistResource( res, XAResource.TMSUCCESS );

        tm.commit();

        tm.resume( tx );
        assertTrue( tm.getStatus() == Status.STATUS_NO_TRANSACTION );
        assertTrue( tm.getTransaction() == null );

        // tm.resume( my fake implementation of transaction );
        // assertTrue( tm.getStatus() == Status.STATUS_NO_TRANSACTION );
        // assertTrue( tm.getTransaction() == null );
    }

    /**
     * o Tests two-phase commits with two different fake XAResource
     * implementations so a branch is created within the same global
     * transaction.
     */
    public void test2PhaseCommits1() throws Exception
    {
        tm.begin();
        FakeXAResource res1 = new FakeXAResource( "XAResource1" );
        FakeXAResource res2 = new FakeXAResource( "XAResource2" );
        // enlist two different resources and verify that the start method
        // is invoked with correct flags
        // res1
        tm.getTransaction().enlistResource( res1 );
        MethodCall calls1[] = res1.getAndRemoveMethodCalls();
        assertEquals( 1, calls1.length );
        assertEquals( "start", calls1[0].getMethodName() );
        // res2
        tm.getTransaction().enlistResource( res2 );
        MethodCall calls2[] = res2.getAndRemoveMethodCalls();
        assertEquals( 1, calls2.length );
        assertEquals( "start", calls2[0].getMethodName() );

        // verify Xid
        Object args[] = calls1[0].getArgs();
        Xid xid1 = (Xid) args[0];
        assertEquals( XAResource.TMNOFLAGS, ((Integer) args[1]).intValue() );
        args = calls2[0].getArgs();
        Xid xid2 = (Xid) args[0];
        assertEquals( XAResource.TMNOFLAGS, ((Integer) args[1]).intValue() );
        // should have same global transaction id
        byte globalTxId1[] = xid1.getGlobalTransactionId();
        byte globalTxId2[] = xid2.getGlobalTransactionId();
        assertTrue( globalTxId1.length == globalTxId2.length );
        for ( int i = 0; i < globalTxId1.length; i++ )
        {
            assertEquals( globalTxId1[i], globalTxId2[i] );
        }
        byte branch1[] = xid1.getBranchQualifier();
        byte branch2[] = xid2.getBranchQualifier();
        // make sure a different branch was created
        if ( branch1.length == branch2.length )
        {
            boolean same = true;
            for ( int i = 0; i < branch1.length; i++ )
            {
                if ( branch1[i] != branch2[i] )
                {
                    same = false;
                    break;
                }
            }
            assertTrue( !same );
        }

        // verify delist of resource
        tm.getTransaction().delistResource( res2, XAResource.TMSUCCESS );
        calls2 = res2.getAndRemoveMethodCalls();
        assertEquals( 1, calls2.length );
        tm.getTransaction().delistResource( res1, XAResource.TMSUCCESS );
        calls1 = res1.getAndRemoveMethodCalls();
        // res1
        assertEquals( 1, calls1.length );
        assertEquals( "end", calls1[0].getMethodName() );
        args = calls1[0].getArgs();
        assertTrue( ((Xid) args[0]).equals( xid1 ) );
        assertEquals( XAResource.TMSUCCESS, ((Integer) args[1]).intValue() );
        // res2
        assertEquals( 1, calls2.length );
        assertEquals( "end", calls2[0].getMethodName() );
        args = calls2[0].getArgs();
        assertTrue( ((Xid) args[0]).equals( xid2 ) );
        assertEquals( XAResource.TMSUCCESS, ((Integer) args[1]).intValue() );

        // verify proper prepare/commit
        tm.commit();
        calls1 = res1.getAndRemoveMethodCalls();
        calls2 = res2.getAndRemoveMethodCalls();
        // res1
        assertEquals( 2, calls1.length );
        assertEquals( "prepare", calls1[0].getMethodName() );
        args = calls1[0].getArgs();
        assertTrue( ((Xid) args[0]).equals( xid1 ) );
        assertEquals( "commit", calls1[1].getMethodName() );
        args = calls1[1].getArgs();
        assertTrue( ((Xid) args[0]).equals( xid1 ) );
        assertEquals( false, ((Boolean) args[1]).booleanValue() );
        // res2
        assertEquals( 2, calls2.length );
        assertEquals( "prepare", calls2[0].getMethodName() );
        args = calls2[0].getArgs();
        assertTrue( ((Xid) args[0]).equals( xid2 ) );
        assertEquals( "commit", calls2[1].getMethodName() );
        args = calls2[1].getArgs();
        assertTrue( ((Xid) args[0]).equals( xid2 ) );
        assertEquals( false, ((Boolean) args[1]).booleanValue() );
    }

    /**
     * o Tests that two enlistments of same resource (according to the
     * isSameRM() method) only receive one set of prepare/commit calls.
     */
    public void test2PhaseCommits2() throws Exception
    {
        tm.begin();
        FakeXAResource res1 = new FakeXAResource( "XAResource1" );
        FakeXAResource res2 = new FakeXAResource( "XAResource1" );
        // enlist two (same) resources and verify that the start method
        // is invoked with correct flags
        // res1
        tm.getTransaction().enlistResource( res1 );
        MethodCall calls1[] = res1.getAndRemoveMethodCalls();
        assertEquals( 1, calls1.length );
        assertEquals( "start", calls1[0].getMethodName() );
        // res2
        tm.getTransaction().enlistResource( res2 );
        MethodCall calls2[] = res2.getAndRemoveMethodCalls();
        assertEquals( 1, calls2.length );
        assertEquals( "start", calls2[0].getMethodName() );

        // make sure we get a two-phase commit
        FakeXAResource res3 = new FakeXAResource( "XAResource2" );
        tm.getTransaction().enlistResource( res3 );

        // verify Xid and flags
        Object args[] = calls1[0].getArgs();
        Xid xid1 = (Xid) args[0];
        assertEquals( XAResource.TMNOFLAGS, ((Integer) args[1]).intValue() );
        args = calls2[0].getArgs();
        Xid xid2 = (Xid) args[0];
        assertEquals( XAResource.TMJOIN, ((Integer) args[1]).intValue() );
        assertTrue( xid1.equals( xid2 ) );
        assertTrue( xid2.equals( xid1 ) );

        // verify delist of resource
        tm.getTransaction().delistResource( res3, XAResource.TMSUCCESS );
        tm.getTransaction().delistResource( res2, XAResource.TMSUCCESS );
        calls2 = res2.getAndRemoveMethodCalls();
        assertEquals( 1, calls2.length );
        tm.getTransaction().delistResource( res1, XAResource.TMSUCCESS );
        calls1 = res1.getAndRemoveMethodCalls();
        // res1
        assertEquals( 1, calls1.length );
        assertEquals( "end", calls1[0].getMethodName() );
        args = calls1[0].getArgs();
        assertTrue( ((Xid) args[0]).equals( xid1 ) );
        assertEquals( XAResource.TMSUCCESS, ((Integer) args[1]).intValue() );
        // res2
        assertEquals( 1, calls2.length );
        assertEquals( "end", calls2[0].getMethodName() );
        args = calls2[0].getArgs();
        assertTrue( ((Xid) args[0]).equals( xid2 ) );
        assertEquals( XAResource.TMSUCCESS, ((Integer) args[1]).intValue() );

        // verify proper prepare/commit
        tm.commit();
        calls1 = res1.getAndRemoveMethodCalls();
        calls2 = res2.getAndRemoveMethodCalls();
        // res1
        assertEquals( 2, calls1.length );
        assertEquals( "prepare", calls1[0].getMethodName() );
        args = calls1[0].getArgs();
        assertTrue( ((Xid) args[0]).equals( xid1 ) );
        assertEquals( "commit", calls1[1].getMethodName() );
        args = calls1[1].getArgs();
        assertTrue( ((Xid) args[0]).equals( xid1 ) );
        assertEquals( false, ((Boolean) args[1]).booleanValue() );
        // res2
        assertEquals( 0, calls2.length );
    }

    /**
     * o Tests that multiple enlistments receive rollback calls properly.
     */
    public void testRollback1() throws Exception
    {
        tm.begin();
        FakeXAResource res1 = new FakeXAResource( "XAResource1" );
        FakeXAResource res2 = new FakeXAResource( "XAResource2" );
        // enlist two different resources and verify that the start method
        // is invoked with correct flags
        // res1
        tm.getTransaction().enlistResource( res1 );
        MethodCall calls1[] = res1.getAndRemoveMethodCalls();
        assertEquals( 1, calls1.length );
        assertEquals( "start", calls1[0].getMethodName() );
        // res2
        tm.getTransaction().enlistResource( res2 );
        MethodCall calls2[] = res2.getAndRemoveMethodCalls();
        assertEquals( 1, calls2.length );
        assertEquals( "start", calls2[0].getMethodName() );

        // verify Xid
        Object args[] = calls1[0].getArgs();
        Xid xid1 = (Xid) args[0];
        assertEquals( XAResource.TMNOFLAGS, ((Integer) args[1]).intValue() );
        args = calls2[0].getArgs();
        Xid xid2 = (Xid) args[0];
        assertEquals( XAResource.TMNOFLAGS, ((Integer) args[1]).intValue() );
        // should have same global transaction id
        byte globalTxId1[] = xid1.getGlobalTransactionId();
        byte globalTxId2[] = xid2.getGlobalTransactionId();
        assertTrue( globalTxId1.length == globalTxId2.length );
        for ( int i = 0; i < globalTxId1.length; i++ )
        {
            assertEquals( globalTxId1[i], globalTxId2[i] );
        }
        byte branch1[] = xid1.getBranchQualifier();
        byte branch2[] = xid2.getBranchQualifier();
        // make sure a different branch was created
        if ( branch1.length == branch2.length )
        {
            boolean same = true;
            for ( int i = 0; i < branch1.length; i++ )
            {
                if ( branch1[i] != branch2[i] )
                {
                    same = false;
                    break;
                }
            }
            assertTrue( !same );
        }

        // verify delist of resource
        tm.getTransaction().delistResource( res2, XAResource.TMSUCCESS );
        calls2 = res2.getAndRemoveMethodCalls();
        assertEquals( 1, calls2.length );
        tm.getTransaction().delistResource( res1, XAResource.TMSUCCESS );
        calls1 = res1.getAndRemoveMethodCalls();
        // res1
        assertEquals( 1, calls1.length );
        assertEquals( "end", calls1[0].getMethodName() );
        args = calls1[0].getArgs();
        assertTrue( ((Xid) args[0]).equals( xid1 ) );
        assertEquals( XAResource.TMSUCCESS, ((Integer) args[1]).intValue() );
        // res2
        assertEquals( 1, calls2.length );
        assertEquals( "end", calls2[0].getMethodName() );
        args = calls2[0].getArgs();
        assertTrue( ((Xid) args[0]).equals( xid2 ) );
        assertEquals( XAResource.TMSUCCESS, ((Integer) args[1]).intValue() );

        // verify proper rollback
        tm.rollback();
        calls1 = res1.getAndRemoveMethodCalls();
        calls2 = res2.getAndRemoveMethodCalls();
        // res1
        assertEquals( 1, calls1.length );
        assertEquals( "rollback", calls1[0].getMethodName() );
        args = calls1[0].getArgs();
        assertTrue( ((Xid) args[0]).equals( xid1 ) );
        // res2
        assertEquals( 1, calls2.length );
        assertEquals( "rollback", calls2[0].getMethodName() );
        args = calls2[0].getArgs();
        assertTrue( ((Xid) args[0]).equals( xid2 ) );
    }

    /*
     * o Tests that multiple enlistments of same (according to isSameRM()
     * method) only receive one set of rollback calls.
     */
    public void testRollback2() throws Exception
    {
        tm.begin();
        FakeXAResource res1 = new FakeXAResource( "XAResource1" );
        FakeXAResource res2 = new FakeXAResource( "XAResource1" );
        // enlist two (same) resources and verify that the start method
        // is invoked with correct flags
        // res1
        tm.getTransaction().enlistResource( res1 );
        MethodCall calls1[] = res1.getAndRemoveMethodCalls();
        assertEquals( 1, calls1.length );
        assertEquals( "start", calls1[0].getMethodName() );
        // res2
        tm.getTransaction().enlistResource( res2 );
        MethodCall calls2[] = res2.getAndRemoveMethodCalls();
        assertEquals( 1, calls2.length );
        assertEquals( "start", calls2[0].getMethodName() );

        // verify Xid and flags
        Object args[] = calls1[0].getArgs();
        Xid xid1 = (Xid) args[0];
        assertEquals( XAResource.TMNOFLAGS, ((Integer) args[1]).intValue() );
        args = calls2[0].getArgs();
        Xid xid2 = (Xid) args[0];
        assertEquals( XAResource.TMJOIN, ((Integer) args[1]).intValue() );
        assertTrue( xid1.equals( xid2 ) );
        assertTrue( xid2.equals( xid1 ) );

        // verify delist of resource
        tm.getTransaction().delistResource( res2, XAResource.TMSUCCESS );
        calls2 = res2.getAndRemoveMethodCalls();
        assertEquals( 1, calls2.length );
        tm.getTransaction().delistResource( res1, XAResource.TMSUCCESS );
        calls1 = res1.getAndRemoveMethodCalls();
        // res1
        assertEquals( 1, calls1.length );
        assertEquals( "end", calls1[0].getMethodName() );
        args = calls1[0].getArgs();
        assertTrue( ((Xid) args[0]).equals( xid1 ) );
        assertEquals( XAResource.TMSUCCESS, ((Integer) args[1]).intValue() );
        // res2
        assertEquals( 1, calls2.length );
        assertEquals( "end", calls2[0].getMethodName() );
        args = calls2[0].getArgs();
        assertTrue( ((Xid) args[0]).equals( xid2 ) );
        assertEquals( XAResource.TMSUCCESS, ((Integer) args[1]).intValue() );

        // verify proper prepare/commit
        tm.rollback();
        calls1 = res1.getAndRemoveMethodCalls();
        calls2 = res2.getAndRemoveMethodCalls();
        // res1
        assertEquals( 1, calls1.length );
        assertEquals( "rollback", calls1[0].getMethodName() );
        args = calls1[0].getArgs();
        assertTrue( ((Xid) args[0]).equals( xid1 ) );
        // res2
        assertEquals( 0, calls2.length );
    }

    /**
     * o Tests if nested transactions are supported
     * 
     * TODO: if supported, do some testing :)
     */
    public void testNestedTransactions() throws Exception
    {
        assertTrue( tm.getTransaction() == null );
        tm.begin();
        Transaction txParent = tm.getTransaction();
        assertTrue( txParent != null );
        try
        {
            tm.begin();
            // ok supported
            // some tests that might be valid for true nested support
            // Transaction txChild = tm.getTransaction();
            // assertTrue( txChild != txParent );
            // tm.commit();
            // assertTrue( txParent == tm.getTransaction() );
        }
        catch ( NotSupportedException e )
        {
            // well no nested transactions
        }
        tm.commit();
        assertTrue( tm.getStatus() == Status.STATUS_NO_TRANSACTION );
    }

    private class TxHook implements javax.transaction.Synchronization
    {
        boolean gotBefore = false;
        boolean gotAfter = false;
        int statusBefore = -1;
        int statusAfter = -1;
        Transaction txBefore = null;
        Transaction txAfter = null;

        public void beforeCompletion()
        {
            try
            {
                statusBefore = tm.getStatus();
                txBefore = tm.getTransaction();
                gotBefore = true;
            }
            catch ( Exception e )
            {
                throw new RuntimeException( "" + e );
            }
        }

        public void afterCompletion( int status )
        {
            try
            {
                statusAfter = status;
                txAfter = tm.getTransaction();
                assertTrue( status == tm.getStatus() );
                gotAfter = true;
            }
            catch ( Exception e )
            {
                throw new RuntimeException( "" + e );
            }
        }
    }

    /**
     * o Tests that beforeCompletion and afterCompletion are invoked. o Tests
     * that the call is made in the same transaction context. o Tests status in
     * before and after methods depending on commit/rollback.
     * 
     * NOTE: Not sure if the check of Status is correct according to
     * specification.
     */
    public void testTransactionHook() throws Exception
    {
        // test for commit
        tm.begin();
        Transaction tx = tm.getTransaction();
        TxHook txHook = new TxHook();
        tx.registerSynchronization( txHook );
        assertEquals( false, txHook.gotBefore );
        assertEquals( false, txHook.gotAfter );
        tm.commit();
        assertEquals( true, txHook.gotBefore );
        assertEquals( true, txHook.gotAfter );
        assertTrue( tx == txHook.txBefore );
        assertTrue( tx == txHook.txAfter );
        assertEquals( Status.STATUS_ACTIVE, txHook.statusBefore );
        assertEquals( Status.STATUS_COMMITTED, txHook.statusAfter );

        // test for rollback
        tm.begin();
        tx = tm.getTransaction();
        txHook = new TxHook();
        tx.registerSynchronization( txHook );
        assertEquals( false, txHook.gotBefore );
        assertEquals( false, txHook.gotAfter );
        tm.rollback();
        assertEquals( true, txHook.gotBefore );
        assertEquals( true, txHook.gotAfter );
        assertTrue( tx == txHook.txBefore );
        assertTrue( tx == txHook.txAfter );
        assertEquals( Status.STATUS_ACTIVE, txHook.statusBefore );
        assertEquals( Status.STATUS_ROLLEDBACK, txHook.statusAfter );
    }

    /**
     * Tests that the correct status is returned from TM.
     * 
     * TODO: Implement a FakeXAResource to check: STATUS_COMMITTING
     * STATUS_PREPARED STATUS_PREPEARING STATUS_ROLLING_BACK
     */
    public void testStatus() throws Exception
    {
        assertTrue( tm.getStatus() == Status.STATUS_NO_TRANSACTION );
        tm.begin();
        assertTrue( tm.getStatus() == Status.STATUS_ACTIVE );
        tm.getTransaction().setRollbackOnly();
        assertTrue( tm.getStatus() == Status.STATUS_MARKED_ROLLBACK );
        tm.rollback();
        assertTrue( tm.getStatus() == Status.STATUS_NO_TRANSACTION );
    }

    /**
     * Is one-phase commit always performed when only one (or many isSameRM)
     * resource(s) are present in the transaction?
     * 
     * If so it could be tested...
     */
    // public void test1PhaseCommit()
    // {
    //	
    // }
    public static class DummyXaDataSource extends XaDataSource
    {
        private XAResource xaResource = null;

        public DummyXaDataSource( java.util.Map<?,?> map )
            throws InstantiationException
        {
            super( map );
            this.xaResource = (XAResource) map.get( "xa_resource" );
        }

        public void close()
        {
        }

        public XaConnection getXaConnection()
        {
            return new DummyXaConnection( xaResource );
        }

        @Override
        public byte[] getBranchId()
        {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void setBranchId( byte[] branchId )
        {
            // TODO Auto-generated method stub

        }
    }

    private static class DummyXaConnection implements XaConnection
    {
        private XAResource xaResource = null;

        public DummyXaConnection( XAResource xaResource )
        {
            this.xaResource = xaResource;
        }

        public XAResource getXaResource()
        {
            return xaResource;
        }

        public void destroy()
        {
        }
    }
}