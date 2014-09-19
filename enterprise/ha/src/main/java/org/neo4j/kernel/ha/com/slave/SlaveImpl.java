/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.IOException;

import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionStream;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.impl.store.StoreId;

public class SlaveImpl implements Slave
{
    private final StoreId storeId;
    private final UpdatePuller puller;

    public SlaveImpl( StoreId storeId, UpdatePuller puller )
    {
        this.storeId = storeId;
        this.puller = puller;
    }

    @Override
    public Response<Void> pullUpdates( long upToAndIncludingTxId )
    {
        try
        {
            puller.pullUpdates();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        return new Response( null, storeId, TransactionStream.EMPTY, ResourceReleaser.NO_OP );
    }

    @Override
    public int getServerId()
    {
        return 0;
    }
}
