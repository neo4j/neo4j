/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

/**
 * Context used by AcceptorState
 */
public class AcceptorContext
{
    private Logging logging;
    private final AcceptorInstanceStore instanceStore;

    public AcceptorContext( Logging logging, AcceptorInstanceStore instanceStore )
    {
        this.logging = logging;
        this.instanceStore = instanceStore;
    }

    public AcceptorInstance getAcceptorInstance( InstanceId instanceId )
    {
        return instanceStore.getAcceptorInstance( instanceId );
    }

    public void promise( AcceptorInstance instance, long ballot )
    {
        instanceStore.promise( instance, ballot );
    }

    public void accept( AcceptorInstance instance, Object value )
    {
        instanceStore.accept( instance, value );
    }

    public StringLogger getLogger( Class clazz )
    {
        return logging.getMessagesLog( clazz );
    }

    public void leave()
    {
        instanceStore.clear();
    }
}
