/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.internal.messaging;

import java.io.IOException;
import java.util.Map;

import org.neo4j.driver.Value;

import static java.lang.String.format;

/**
 * SUCCESS response message
 * <p>
 * Sent by the server to signal a successful operation.
 * Terminates response sequence.
 */
public class SuccessMessage implements Message
{
    private final Map<String,Value> metadata;

    public SuccessMessage( Map<String,Value> metadata )
    {
        this.metadata = metadata;
    }

    @Override
    public void dispatch( MessageHandler handler ) throws IOException
    {
        handler.handleSuccessMessage( metadata );
    }

    @Override
    public String toString()
    {
        return format( "[SUCCESS %s]", metadata );
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj != null && obj.getClass() == getClass();
    }

    @Override
    public int hashCode()
    {
        return 1;
    }
}
