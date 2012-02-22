/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.neo4j.com.Client.ConnectionLostHandler;
import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;

public interface ResponseReceiver extends ConnectionLostHandler
{
    /**
     * Returns a {@link SlaveContext} instance that has {@code eventIdentifier}
     * as the event identifier.
     *
     * @param eventIdentifier The event identifier of the returned slave context
     * @return The slave context
     */
    SlaveContext getSlaveContext( int eventIdentifier );

    <T> T receive( Response<T> response );

    void newMaster( Exception cause );

    void reconnect( Exception cause );

    int getMasterForTx( long tx );
}
