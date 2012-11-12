/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.event;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.transaction.XidImpl;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaResource;

public class IllBehavingXaDataSource extends XaDataSource
{
    IllBehavingXaDataSource() throws InstantiationException
    {
        super( MapUtil.stringMap( "store_dir", "target/var" ) );
    }
    
    @Override
    public void close()
    {
    }

    @Override
    public XaConnection getXaConnection()
    {
        return new IllBehavingXaConnection();
    }

    private static class IllBehavingXaConnection implements XaConnection
    {
        public void destroy()
        {
            // TODO Auto-generated method stub
            
        }

        public XAResource getXaResource()
        {
            return new IllBehavingXaResource();
        }
    }
    
    private static class IllBehavingXaResource implements XaResource
    {
        public byte[] getBranchId()
        {
            return UTF8.encode( "554342" );
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
            if ( xaResource instanceof IllBehavingXaResource )
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