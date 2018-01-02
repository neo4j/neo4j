/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

final class FileVersion
{
    static final long INITIAL_TX_ID = TransactionIdStore.BASE_TX_ID;
    static final int INITIAL_MINOR_VERSION = 0;
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

    public FileVersion( long txId )
    {
        this( txId, INITIAL_MINOR_VERSION );
    }

    public FileVersion update( long txId )
    {
        return new FileVersion( txId, this.txId == txId ? minorVersion + 1 : INITIAL_MINOR_VERSION );
    }

    @Override
    public String toString()
    {
        return String.format( "FileVersion[txId=%d, minorVersion=%d]", txId, minorVersion );
    }

    FileVersion( long txId, long minorVersion )
    {

        this.txId = txId;
        this.minorVersion = minorVersion;
    }
}
