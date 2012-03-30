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

package org.neo4j.kernel.impl.index;

import java.util.Map;
import javax.transaction.xa.XAResource;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.kernel.impl.transaction.xaframework.LogBackedXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnectionHelpImpl;
import org.neo4j.kernel.impl.transaction.xaframework.XaResourceHelpImpl;
import org.neo4j.kernel.impl.transaction.xaframework.XaResourceManager;

public class DummyIndexDataSource extends LogBackedXaDataSource
{
    public DummyIndexDataSource( byte[] branchId, String name )
    {
        super( branchId, name );
    }

    @Override
    public XaConnection getXaConnection()
    {
        return null;
    }

    @Override
    public void close()
    {
    }
    
    public static class DummyConnection extends XaConnectionHelpImpl implements IndexXaConnection
    {
        private final XAResource resource;
        
        public DummyConnection( XaResourceManager xaRm, byte[] branchId )
        {
            super( xaRm );
            resource = new DummyResource( "some identifier", xaRm, branchId );
        }

        @Override
        public void createIndex( Class<? extends PropertyContainer> entityType, String indexName,
                Map<String, String> config )
        {
        }

        @Override
        public XAResource getXaResource()
        {
            return resource;
        }
    }
    
    private static class DummyResource extends XaResourceHelpImpl
    {
        private final Object identifier;
        
        DummyResource( Object identifier, XaResourceManager xaRm, 
            byte[] branchId )
        {
            super( xaRm, branchId );
            this.identifier = identifier;
        }
        
        @Override
        public boolean isSameRM( XAResource xares )
        {
            if ( xares instanceof DummyResource )
            {
                return identifier.equals( 
                    ((DummyResource) xares).identifier );
            }
            return false;
        }
    }
}
