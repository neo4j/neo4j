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
package org.neo4j.consistency.store.windowpool;

import java.io.IOException;

import org.neo4j.kernel.impl.nioneo.store.StoreChannel;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

public class FileMapper
{
    private final StoreChannel fileChannel;

    public FileMapper( StoreChannel fileChannel )
    {
        this.fileChannel = fileChannel;
    }

    public long fileSizeInBytes() throws IOException
    {
        return fileChannel.size();
    }

    public MappedWindow mapWindow( long firstRecord, int recordsPerPage, int bytesPerRecord ) throws IOException
    {
        return new MappedWindow( recordsPerPage, bytesPerRecord, firstRecord,
                fileChannel.map( READ_ONLY, firstRecord * bytesPerRecord, recordsPerPage * bytesPerRecord ) );
    }
}
