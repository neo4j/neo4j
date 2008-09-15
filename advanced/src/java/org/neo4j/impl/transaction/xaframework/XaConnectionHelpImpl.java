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
package org.neo4j.impl.transaction.xaframework;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

/**
 * <CODE>XaConnectionHelpImpl</CODE> helps with basic implementation that is
 * needed to fit a non XA compatible resource in the <CODE>xaframework</CODE>.
 * <p>
 * Put your methods that perform any transactional work towards the resource in
 * your extention of this class. Create a instance of the
 * {@link XaResourceHelpImpl} as your <CODE>XAResource</CODE>. Add a
 * try-finally block in your work methods around the command creations and
 * enlist/delist the <CODE>XAResource</CODE> with the transaction. Use the
 * protected <CODE>getTransaction</CODE> method to get a {@link XaTransaction}
 * and add your commands to the transaction. Here is an example implementation:
 * <p>
 * 
 * <pre>
 * <CODE>
 * public class MyConnection extends XaConnectionHelpImpl
 * {
 *     private static class MyResource extends XaResourceHelpImpl
 *     {
 *         public boolean isSameRM( XAResource rm )
 *         {
 *             if ( rm instanceof MyResource )
 *             {
 *                 return true;
 *             }
 *             return false;
 *         }
 *     }
 * 
 *     private XaResource xaResource = new MyResource();
 * 
 *     public XaResource getXaResource()
 *     {
 *         return xaResource;
 *     }
 * 
 *     public void doWork1() throws SomeException
 *     {
 *         enlistResourceWithTx();
 *         try
 *         {
 *             getTransaction().addCommand( new Work1Command( params ) );
 *         }
 *         finally
 *         {
 *             delistResourceFromTx();
 *         }
 *     }
 * }
 * </CODE>
 * </pre>
 */
public abstract class XaConnectionHelpImpl implements XaConnection
{
    private final XaResourceManager xaRm;

    public XaConnectionHelpImpl( XaResourceManager xaRm )
    {
        if ( xaRm == null )
        {
            throw new IllegalArgumentException( "XaResourceManager is null" );
        }
        this.xaRm = xaRm;
    }

    /**
     * Returns the XAResource associated with this connection.
     * 
     * @return The XAResource for this connection
     */
    public abstract XAResource getXaResource();

    public void destroy()
    {
        // kill xaResource
        xaRm.destroy( getXaResource() );
    }

    /**
     * Makes sure the resource is enlisted as active in the transaction.
     * 
     * @throws XAException
     *             If resource not enlisted or suspended
     */
    public void validate() throws XAException
    {
        xaRm.validate( getXaResource() );
    }

    /**
     * Returns the {@link XaTransaction} associated with this connection. If
     * transaction is already completed it will still be returned.
     * 
     * @return The {@link XaTransaction} associated with this connection
     * @throws XAException
     *             If the transaction hasn't completed and the resource isn't
     *             enlisted
     */
    protected XaTransaction getTransaction() throws XAException
    {
        XAResource xar = getXaResource();
        XaTransaction xat = null;
        if ( xar instanceof XaResourceHelpImpl )
        {
            xat = ((XaResourceHelpImpl) xar).getCompletedTx();
        }
        if ( xat != null )
        {
            return xat;
        }
        return xaRm.getXaTransaction( xar );
    }

    /**
     * Will clear the resource manager of all transactions. Used for testing
     * purpose only. Do not use this method unless you know what you're doing
     * since it will corrupt the state between the resource and the global
     * transaction manager.
     */
    public void clearAllTransactions()
    {
        xaRm.reset();
    }
}