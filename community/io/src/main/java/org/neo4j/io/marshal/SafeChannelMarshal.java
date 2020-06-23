/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.io.marshal;

import java.io.IOException;

import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.fs.ReadableChannel;

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
