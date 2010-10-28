/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package slavetest;

import java.io.Serializable;
import java.rmi.RemoteException;

import org.neo4j.shell.impl.RmiLocation;

public class MultiJvmDLFetcher implements Fetcher<DoubleLatch>, Serializable
{
    public MultiJvmDLFetcher() throws RemoteException
    {
        MultiJvmDoubleLatch latch = new MultiJvmDoubleLatch();
        RmiLocation location = location();
        location.ensureRegistryCreated();
        location.bind( latch );
    }
    
    private RmiLocation location()
    {
        return RmiLocation.location( "localhost", 8054, "latch" );
    }
    
    public DoubleLatch fetch()
    {
        try
        {
            return (DoubleLatch) location().getBoundObject();
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void close()
    {
        // TODO
    }
}
