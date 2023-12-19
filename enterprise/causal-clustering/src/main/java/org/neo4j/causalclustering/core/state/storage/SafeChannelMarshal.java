/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
