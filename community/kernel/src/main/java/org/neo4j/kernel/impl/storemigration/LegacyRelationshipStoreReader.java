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
import java.util.Iterator;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

public class LegacyRelationshipStoreReader
{
    public static final int RECORD_LENGTH = 33;

    private final FileChannel fileChannel;
    private final long maxId;

    public LegacyRelationshipStoreReader( String fileName ) throws IOException
    {
        fileChannel = new RandomAccessFile( fileName, "r" ).getChannel();
        int endHeaderSize = UTF8.encode( FROM_VERSION ).length;
        maxId = (fileChannel.size() - endHeaderSize) / RECORD_LENGTH;
    }

    public Iterable<RelationshipRecord> readRelationshipStore() throws IOException
    {

        final ByteBuffer buffer = ByteBuffer.allocateDirect( RECORD_LENGTH );

        return new Iterable<RelationshipRecord>()
        {
            @Override
            public Iterator<RelationshipRecord> iterator()
            {
                return new Iterator<RelationshipRecord>()
                {
                    long id = 0;

                    @Override
                    public boolean hasNext()
                    {
                        return id < maxId;
                    }

                    @Override
                    public RelationshipRecord next()
                    {
                        RelationshipRecord record = null;
                        do
                        {
                            buffer.position( 0 );
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

                                record = new RelationshipRecord( id,
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
                            }
                            id++;
                        } while ( record == null && id < maxId );

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
