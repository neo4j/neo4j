/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.ha.com.slave;

import org.neo4j.com.Response;
import org.neo4j.com.storecopy.TransactionObligationFulfiller;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.ha.com.master.Slave;

public class SlaveImpl implements Slave
{
    private final TransactionObligationFulfiller fulfiller;

    public SlaveImpl( TransactionObligationFulfiller fulfiller )
    {
        this.fulfiller = fulfiller;
    }

    @Override
    public Response<Void> pullUpdates( long upToAndIncludingTxId )
    {
        try
        {
            fulfiller.fulfill( upToAndIncludingTxId );
        }
        catch ( InterruptedException e )
        {
            throw Exceptions.launderedException( e );
        }
        return Response.EMPTY;
    }

    @Override
    public int getServerId()
    {
        throw new UnsupportedOperationException( "This should not be called. Knowing the server id is only needed " +
                "on the client side, we're now on the server side." );
    }
}
