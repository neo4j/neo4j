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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public class LegacyRelationshipStoreReader
{
    public static final String FROM_VERSION = "RelationshipStore v0.9.9";
    public static final int RECORD_LENGTH = 33;

    private final FileChannel fileChannel;
    private final long maxId;

    public LegacyRelationshipStoreReader( File fileName ) throws IOException
    {
        fileChannel = new RandomAccessFile( fileName, "r" ).getChannel();
        int endHeaderSize = UTF8.encode( FROM_VERSION ).length;
        maxId = (fileChannel.size() - endHeaderSize) / RECORD_LENGTH;
    }

    public long getMaxId()
    {
        return maxId;
    }

    public Iterable<RelationshipRecord> readRelationshipStore() throws IOException
    {
        final ByteBuffer buffer = ByteBuffer.allocateDirect( RECORD_LENGTH );

        return new Iterable<RelationshipRecord>()
        {
            @Override
            public Iterator<RelationshipRecord> iterator()
            {
                return new PrefetchingIterator<RelationshipRecord>()
                {
                    long id = 0;

                    @Override
                    protected RelationshipRecord fetchNextOrNull()
                    {
                        RelationshipRecord record = null;
                        while ( record == null && id <= maxId )
                        {
                            buffer.clear();
                            try
                            {
                                fileChannel.read( buffer );
                            } catch ( IOException e )
                            {
                                throw new RuntimeException( e );
                            }
                            buffer.flip();
                            long inUseByte = buffer.get();

                            boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();
                            if ( inUse )
                            {
                                long firstNode = LegacyStore.getUnsignedInt( buffer );
                                long firstNodeMod = (inUseByte & 0xEL) << 31;

                                long secondNode = LegacyStore.getUnsignedInt( buffer );

                                // [ xxx,    ][    ,    ][    ,    ][    ,    ] second node high order bits,     0x70000000
                                // [    ,xxx ][    ,    ][    ,    ][    ,    ] first prev rel high order bits,  0xE000000
                                // [    ,   x][xx  ,    ][    ,    ][    ,    ] first next rel high order bits,  0x1C00000
                                // [    ,    ][  xx,x   ][    ,    ][    ,    ] second prev rel high order bits, 0x380000
                                // [    ,    ][    , xxx][    ,    ][    ,    ] second next rel high order bits, 0x70000
                                // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
                                long typeInt = buffer.getInt();
                                long secondNodeMod = (typeInt & 0x70000000L) << 4;
                                int type = (int) (typeInt & 0xFFFF);

                                record = new RelationshipRecord( id,
                                        LegacyStore.longFromIntAndMod( firstNode, firstNodeMod ),
                                        LegacyStore.longFromIntAndMod( secondNode, secondNodeMod ), type );
                                record.setInUse( inUse );

                                long firstPrevRel = LegacyStore.getUnsignedInt( buffer );
                                long firstPrevRelMod = (typeInt & 0xE000000L) << 7;
                                record.setFirstPrevRel( LegacyStore.longFromIntAndMod( firstPrevRel, firstPrevRelMod ) );

                                long firstNextRel = LegacyStore.getUnsignedInt( buffer );
                                long firstNextRelMod = (typeInt & 0x1C00000L) << 10;
                                record.setFirstNextRel( LegacyStore.longFromIntAndMod( firstNextRel, firstNextRelMod ) );

                                long secondPrevRel = LegacyStore.getUnsignedInt( buffer );
                                long secondPrevRelMod = (typeInt & 0x380000L) << 13;
                                record.setSecondPrevRel( LegacyStore.longFromIntAndMod( secondPrevRel, secondPrevRelMod ) );

                                long secondNextRel = LegacyStore.getUnsignedInt( buffer );
                                long secondNextRelMod = (typeInt & 0x70000L) << 16;
                                record.setSecondNextRel( LegacyStore.longFromIntAndMod( secondNextRel, secondNextRelMod ) );

                                long nextProp = LegacyStore.getUnsignedInt( buffer );
                                long nextPropMod = (inUseByte & 0xF0L) << 28;

                                record.setNextProp( LegacyStore.longFromIntAndMod( nextProp, nextPropMod ) );
                            }
                            else
                            {
                                record = new RelationshipRecord( id, -1, -1, -1 );
                                record.setInUse( false );
                            }
                            id++;
                        }

                        return record;
                    }

                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public void close() throws IOException
    {
        fileChannel.close();
    }
}
