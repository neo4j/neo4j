package org.neo4j.kernel.impl.event;

import java.util.HashMap;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.neo4j.kernel.impl.transaction.XidImpl;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaResource;

public class DummyXaDataSource extends XaDataSource
{
    DummyXaDataSource() throws InstantiationException
    {
        super( new HashMap<Object,Object>() );
    }
    
    @Override
    public void close()
    {
    }

    @Override
    public XaConnection getXaConnection()
    {
        return new NoobXaConnection();
    }

    private static class NoobXaConnection implements XaConnection
    {
        public void destroy()
        {
            // TODO Auto-generated method stub
            
        }

        public XAResource getXaResource()
        {
            return new NoobXaResource();
        }
    }
    
    private static class NoobXaResource implements XaResource
    {
        public byte[] getBranchId()
        {
            return "554342".getBytes();
        }

        public void setBranchId( byte[] branchId )
        {
        }

        public void commit( Xid xid, boolean onePhase ) throws XAException
        {
            throw new XAException();
        }

        public void end( Xid xid, int flags ) throws XAException
        {
        }

        public void forget( Xid xid ) throws XAException
        {
        }

        public int getTransactionTimeout() throws XAException
        {
            return 0;
        }

        public boolean isSameRM( XAResource xaResource ) throws XAException
        {
            if ( xaResource instanceof NoobXaResource )
            {
                return true;
            }
            return false;
        }

        public int prepare( Xid xid ) throws XAException
        {
            // TODO Auto-generated method stub
            return XAResource.XA_OK;
        }

        public Xid[] recover( int flag ) throws XAException
        {
            return new XidImpl[0];
        }

        public void rollback( Xid xid ) throws XAException
        {
            throw new RuntimeException( "I am a noob" );
        }

        public boolean setTransactionTimeout( int seconds ) throws XAException
        {
            // TODO Auto-generated method stub
            return false;
        }

        public void start( Xid xid, int flags ) throws XAException
        {
        }
    }
}