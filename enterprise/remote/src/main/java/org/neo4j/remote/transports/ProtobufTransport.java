/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.remote.transports;

import java.net.URI;

import org.neo4j.remote.ConnectionTarget;
import org.neo4j.remote.RemoteConnection;
import org.neo4j.remote.Transport;

/*public*/final class ProtobufTransport extends Transport
{
    public ProtobufTransport()
    {
        super( "protobuf" );
    }

    @Override
    protected boolean handlesUri( URI resourceUri )
    {
        return "protobuf".equals( resourceUri.getScheme() );
    }

    @Override
    protected ConnectionTarget create( URI resourceUri )
    {
        String scheme = resourceUri.getScheme();
        if ( "protobuf".equals( scheme ) )
        {
            return protobuf( resourceUri.getSchemeSpecificPart() );
        }
        else
        {
            throw new IllegalArgumentException( "Unsupported protocol scheme: "
                                                + scheme );
        }
    }

    private ConnectionTarget protobuf( String target )
    {
        return new ConnectionTarget()
        {
            public RemoteConnection connect( String username, String password )
            {
                // TODO Auto-generated method stub
                return connect();
            }

            public RemoteConnection connect()
            {
                // TODO Auto-generated method stub
                return new ProtobufConnection();
            }
        };
    }
}
