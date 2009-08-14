/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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
package org.neo4j.impl.transaction.xaframework;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * Help implementation of the XAResource interface. See
 * {@link XaConnectionHelpImpl} for more information.
 */
public abstract class XaResourceHelpImpl implements XaResource
{
    private int transactionTimeout = 120;
    private XaTransaction xaTx = null;
    private final XaResourceManager xaRm;
    private byte[] branchId = null;

    protected XaResourceHelpImpl( XaResourceManager xaRm, byte branchId[] )
    {
        this.xaRm = xaRm;
        this.branchId = branchId;
    }

    /**
     * If the transaction commited successfully this method will return the
     * transaction.
     * 
     * @return transaction if completed else <CODE>null</CODE>
     */
    public XaTransaction getCompletedTx()
    {
        return xaTx;
    }

    /**
     * Should return true if <CODE>xares</CODE> is the same resource as 
     * <CODE>this</CODE>.
     * 
     * @return true if the resource is same
     */
    public abstract boolean isSameRM( XAResource xares );

    public void commit( Xid xid, boolean onePhase ) throws XAException
    {
        xaTx = xaRm.commit( xid, onePhase );
    }

    public void end( Xid xid, int flags ) throws XAException
    {
        if ( flags == XAResource.TMSUCCESS )
        {
            xaRm.end( this, xid );
        }
        else if ( flags == XAResource.TMSUSPEND )
        {
            xaRm.suspend( xid );
        }
        else if ( flags == XAResource.TMFAIL )
        {
            xaRm.fail( this, xid );
        }
    }

    public void forget( Xid xid ) throws XAException
    {
        xaRm.forget( xid );
    }

    public int getTransactionTimeout()
    {
        return transactionTimeout;
    }

    public boolean setTransactionTimeout( int timeout )
    {
        transactionTimeout = timeout;
        return true;
    }

    public int prepare( Xid xid ) throws XAException
    {
        return xaRm.prepare( xid );
    }

    public Xid[] recover( int flag ) throws XAException
    {
        return xaRm.recover( flag );
    }

    public void rollback( Xid xid ) throws XAException
    {
        xaRm.rollback( xid );
    }

    public void start( Xid xid, int flags ) throws XAException
    {
        xaTx = null;
        if ( flags == XAResource.TMNOFLAGS )
        {
            xaRm.start( this, xid );
        }
        else if ( flags == XAResource.TMRESUME )
        {
            xaRm.resume( xid );
        }
        else if ( flags == XAResource.TMJOIN )
        {
            xaRm.join( this, xid );
        }
        else
        {
            throw new XAException( "Unknown flag[" + flags + "]" );
        }
    }

    public void setBranchId( byte branchId[] )
    {
        this.branchId = branchId;
    }

    public byte[] getBranchId()
    {
        return this.branchId;
    }
}