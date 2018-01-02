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
package org.neo4j.causalclustering.core.state.storage;

import java.io.IOException;

import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.storageengine.api.ReadPastEndException;
import org.neo4j.storageengine.api.ReadableChannel;

/**
 * Wrapper class to handle ReadPastEndExceptions in a safe manner transforming it
 * to the checked EndOfStreamException which does not inherit from an IOException.
 *
 * @param <STATE> The type of state marshalled.
 */
public abstract class SafeChannelMarshal<STATE> implements ChannelMarshal<STATE>
{
    @Override
    public final STATE unmarshal( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        try
        {
            return unmarshal0( channel );
        }
        catch ( ReadPastEndException e )
        {
            throw new EndOfStreamException( e );
        }
    }

    /**
     * The specific implementation of unmarshal which does not have to deal
     * with the IOException {@link ReadPastEndException} and can safely throw
     * the checked EndOfStreamException.
     *
     * @param channel The channel to read from.
     * @return An unmarshalled object.
     * @throws IOException
     * @throws EndOfStreamException
     */
    protected abstract STATE unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException;
}
