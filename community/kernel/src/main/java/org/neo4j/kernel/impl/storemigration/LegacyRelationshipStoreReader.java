/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import static org.neo4j.kernel.impl.storemigration.LegacyStore.FROM_VERSION;
import static org.neo4j.kernel.impl.storemigration.LegacyStore.getUnsignedInt;
import static org.neo4j.kernel.impl.storemigration.LegacyStore.longFromIntAndMod;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public class LegacyRelationshipStoreReader
{
    private String fileName;

    public LegacyRelationshipStoreReader( String fileName )
    {
        this.fileName = fileName;
    }

    public Iterable<RelationshipRecord> readRelationshipStore() throws IOException
    {
        FileChannel fileChannel = new RandomAccessFile( fileName, "r" ).getChannel();
        int recordLength = 33;
        int endHeaderSize = UTF8.encode( FROM_VERSION ).length;
        long recordCount = (fileChannel.size() - endHeaderSize) / recordLength;

        ByteBuffer buffer = ByteBuffer.allocateDirect( recordLength );

        ArrayList<RelationshipRecord> records = new ArrayList<RelationshipRecord>();
        for ( long id = 0; id < recordCount; id++ )
        {
            buffer.position( 0 );
            fileChannel.read( buffer );
            buffer.flip();
            long inUseByte = buffer.get();

            boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();

            if ( inUse )
            {
                long firstNode = getUnsignedInt( buffer );
                long firstNodeMod = (inUseByte & 0xEL) << 31;

                long secondNode = getUnsignedInt( buffer );

                // [ xxx,    ][    ,    ][    ,    ][    ,    ] second node high order bits,     0x70000000
                // [    ,xxx ][    ,    ][    ,    ][    ,    ] first prev rel high order bits,  0xE000000
                // [    ,   x][xx  ,    ][    ,    ][    ,    ] first next rel high order bits,  0x1C00000
                // [    ,    ][  xx,x   ][    ,    ][    ,    ] second prev rel high order bits, 0x380000
                // [    ,    ][    , xxx][    ,    ][    ,    ] second next rel high order bits, 0x70000
                // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
                long typeInt = buffer.getInt();
                long secondNodeMod = (typeInt & 0x70000000L) << 4;
                int type = (int) (typeInt & 0xFFFF);

                RelationshipRecord record = new RelationshipRecord( id,
                        longFromIntAndMod( firstNode, firstNodeMod ),
                        longFromIntAndMod( secondNode, secondNodeMod ), type );
                record.setInUse( inUse );

                long firstPrevRel = getUnsignedInt( buffer );
                long firstPrevRelMod = (typeInt & 0xE000000L) << 7;
                record.setFirstPrevRel( longFromIntAndMod( firstPrevRel, firstPrevRelMod ) );

                long firstNextRel = getUnsignedInt( buffer );
                long firstNextRelMod = (typeInt & 0x1C00000L) << 10;
                record.setFirstNextRel( longFromIntAndMod( firstNextRel, firstNextRelMod ) );

                long secondPrevRel = getUnsignedInt( buffer );
                long secondPrevRelMod = (typeInt & 0x380000L) << 13;
                record.setSecondPrevRel( longFromIntAndMod( secondPrevRel, secondPrevRelMod ) );

                long secondNextRel = getUnsignedInt( buffer );
                long secondNextRelMod = (typeInt & 0x70000L) << 16;
                record.setSecondNextRel( longFromIntAndMod( secondNextRel, secondNextRelMod ) );

                long nextProp = getUnsignedInt( buffer );
                long nextPropMod = (inUseByte & 0xF0L) << 28;

                record.setNextProp( longFromIntAndMod( nextProp, nextPropMod ) );

                records.add( record );
            }
        }
        return records;
    }

}
