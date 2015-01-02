/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

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
public class OtherDummyXaDataSource extends XaDataSource
{
    private XAResource xaResource = null;

    public OtherDummyXaDataSource( String name, byte[] branchId, XAResource xaResource )
    {
        super( branchId, name );
        this.xaResource = xaResource;
    }

    @Override
    public XaConnection getXaConnection()
    {
        return new OtherDummyXaConnection( xaResource );
    }

    @Override
    public long getLastCommittedTxId()
    {
        return 0;
    }

    @Override
    public void init()
    {
    }

    @Override
    public void start()
    {
    }

    @Override
    public void stop()
    {
    }

    @Override
    public void shutdown()
    {
    }

    private static class OtherDummyXaConnection implements XaConnection
    {
        private XAResource xaResource = null;

        public OtherDummyXaConnection( XAResource xaResource )
        {
            this.xaResource = xaResource;
        }

        @Override
        public XAResource getXaResource()
        {
            return xaResource;
        }

        @Override
        public void destroy()
        {
        }

        @Override
        public boolean enlistResource( Transaction javaxTx )
            throws SystemException, RollbackException
        {
            return javaxTx.enlistResource( xaResource );
        }

        @Override
        public boolean delistResource( Transaction tx, int tmsuccess )
            throws IllegalStateException, SystemException
        {
            return tx.delistResource( xaResource, tmsuccess );
        }
    }
}
