/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration.legacystore;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

public class LegacyRelationshipTypeStoreReader
{
    public static final String FROM_VERSION = "RelationshipTypeStore v0.9.9";
    private final File fileName;
    private final FileSystemAbstraction fs;

    public LegacyRelationshipTypeStoreReader( FileSystemAbstraction fs, File fileName )
    {
        this.fs = fs;
        this.fileName = fileName;
    }

    public Iterable<RelationshipTypeRecord> readRelationshipTypes() throws IOException
    {
        FileChannel fileChannel = fs.open( fileName, "r" );
        int recordLength = 5;
        int endHeaderSize = UTF8.encode( FROM_VERSION ).length;
        long recordCount = (fileChannel.size() - endHeaderSize) / recordLength;

        LinkedList<RelationshipTypeRecord> records = new LinkedList<RelationshipTypeRecord>();

        ByteBuffer buffer = ByteBuffer.allocateDirect( recordLength );

        for ( int id = 0; id <= recordCount; id++ )
        {
            buffer.position( 0 );
            fileChannel.read( buffer );
            buffer.flip();
            long inUseByte = buffer.get();

            boolean inUse = inUseByte == Record.IN_USE.byteValue();
            if (inUse) {
                RelationshipTypeRecord record = new RelationshipTypeRecord( id );
                record.setInUse( inUse );
                record.setNameId( buffer.getInt() );
                records.add( record );
            }
        }
        fileChannel.close();
        return records;
    }
}
