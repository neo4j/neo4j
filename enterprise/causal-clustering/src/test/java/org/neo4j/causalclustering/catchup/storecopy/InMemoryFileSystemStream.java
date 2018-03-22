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
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class InMemoryFileSystemStream implements StoreFileStreams
{
    Map<String,StringBuffer> filesystem = new HashMap<>();

    /**
     *
     * @param destination
     * @param requiredAlignment
     * @param data
     * @throws IOException
     */
    public void write( String destination, int requiredAlignment, byte[] data ) throws IOException
    {
        StringBuffer buffer = filesystem.getOrDefault( destination, new StringBuffer() );
        for ( byte b : data )
        {
            buffer.append( (char) b );
        }
        filesystem.put( destination, buffer );
    }

    @Override
    public void close() throws Exception
    {
        throw new RuntimeException( "Unimplemented" );
    }

    public Map<String,StringBuffer> getFilesystem()
    {
        return filesystem;
    }
}
