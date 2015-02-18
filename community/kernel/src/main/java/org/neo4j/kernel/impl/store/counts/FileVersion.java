/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

package org.neo4j.kernel.impl.store.counts;

import org.neo4j.kernel.impl.store.kvstore.HeaderField;
import org.neo4j.kernel.impl.store.kvstore.ReadableBuffer;
import org.neo4j.kernel.impl.store.kvstore.WritableBuffer;

final class FileVersion
{
    final long txId;
    final long minorVersion;
    static final HeaderField<FileVersion> FILE_VERSION = new HeaderField<FileVersion>()
    {
        @Override
        public FileVersion read( ReadableBuffer header )
        {
            return new FileVersion( header.getLong( 0 ), header.getLong( 8 ) );
        }

        @Override
        public void write( FileVersion the, WritableBuffer header )
        {
            header.putLong( 0, the.txId );
            header.putLong( 8, the.minorVersion );
        }

        @Override
        public String toString()
        {
            return "<Transaction ID>";
        }
    };

    FileVersion( long txId, long minorVersion )
    {

        this.txId = txId;
        this.minorVersion = minorVersion;
    }

    FileVersion update( Change changes )
    {
        return new FileVersion( changes.txId, this.txId == changes.txId ? minorVersion + 1 : 1 );
    }

    static class Change
    {
        final long txId;

        Change( long txId )
        {
            this.txId = txId;
        }
    }
}
