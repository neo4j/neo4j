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

package org.neo4j.kernel.ha;

import java.nio.channels.ReadableByteChannel;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.zookeeper.Machine;

public class DelegatingResponseReceiver implements ResponseReceiver
{
    private ResponseReceiver target;

    public void setTarget( ResponseReceiver target )
    {
        this.target = target;
    }
    
    @Override
    public SlaveContext getSlaveContext( int eventIdentifier )
    {
        return target.getSlaveContext( eventIdentifier );
    }

    @Override
    public <T> T receive( Response<T> response )
    {
        return target.receive( response );
    }

    @Override
    public void applyTransaction( String datasourceName, long txId, ReadableByteChannel stream )
    {
        target.applyTransaction( datasourceName, txId, stream );
    }

    @Override
    public void newMaster( Pair<Master, Machine> master, Exception e )
    {
        target.newMaster( master, e );
    }
}
