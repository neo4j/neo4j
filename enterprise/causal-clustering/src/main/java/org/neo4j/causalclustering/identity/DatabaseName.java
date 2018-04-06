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
package org.neo4j.causalclustering.identity;

import java.io.IOException;
import java.util.Objects;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.state.storage.SafeStateMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;
import org.neo4j.string.UTF8;

/**
 * Simple wrapper class for database name strings. These values are provided using the
 * {@link CausalClusteringSettings#database } setting.
 */
public class DatabaseName
{
    private final String name;

    public DatabaseName( String name )
    {
        this.name = name;
    }

    public String name()
    {
        return name;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        DatabaseName that = (DatabaseName) o;
        return Objects.equals( name, that.name );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( name );
    }

    /**
     * Format:
     *
     * nullMarker: 1 byte
     * nameLength: 1 byte
     * nameBytes: <= 127 bytes
     */
    public static class Marshal extends SafeStateMarshal<DatabaseName>
    {
        @Override
        protected DatabaseName unmarshal0( ReadableChannel channel ) throws IOException
        {
            byte nullMarker = channel.get();
            if ( nullMarker == 0 )
            {
                return null;
            }
            else
            {
                int nameLength = (int) channel.get();
                byte[] nameBytes = new byte[nameLength];
                channel.get( nameBytes, nameLength );
                return new DatabaseName( UTF8.decode( nameBytes ) );
            }
        }

        @Override
        public DatabaseName startState()
        {
            return null;
        }

        @Override
        public long ordinal( DatabaseName databaseName )
        {
            return databaseName == null ? 0 : 1;
        }

        @Override
        public void marshal( DatabaseName databaseName, WritableChannel channel ) throws IOException
        {
            if ( databaseName == null )
            {
                channel.put( (byte) 0 );
            }
            else if ( databaseName.name().length() > 127 )
            {
                throw new IOException( "The database name is too large to be stored. It must be fewer than 128 UTF-8 characters." );
            }
            else
            {
                channel.put( (byte) 1 );
                channel.put( (byte) databaseName.name().length() );
                channel.put( UTF8.encode( databaseName.name() ), databaseName.name().length() );
            }
        }
    }
}
