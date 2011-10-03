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
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;

public class LegacyNodeStoreReader
{
    private String fileName;

    public LegacyNodeStoreReader( String fileName )
    {
        this.fileName = fileName;
    }

    public Iterable<NodeRecord> readNodeStore() throws IOException
    {
        final FileChannel fileChannel = new RandomAccessFile( fileName, "r" ).getChannel();
        int recordLength = 9;
        int endHeaderSize = UTF8.encode( FROM_VERSION ).length;
        final long maxId = (fileChannel.size() - endHeaderSize) / recordLength;

        final ByteBuffer buffer = ByteBuffer.allocateDirect( recordLength );

        return new Iterable<NodeRecord>()
        {
            @Override
            public Iterator<NodeRecord> iterator()
            {
                return new Iterator<NodeRecord>()
                {
                    long id = 0;

                    @Override
                    public boolean hasNext()
                    {
                        return id < maxId;
                    }

                    @Override
                    public NodeRecord next()
                    {
                        NodeRecord nodeRecord = null;
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
                                long nextRel = getUnsignedInt( buffer );
                                long nextProp = getUnsignedInt( buffer );

                                long relModifier = (inUseByte & 0xEL) << 31;
                                long propModifier = (inUseByte & 0xF0L) << 28;

                                nodeRecord = new NodeRecord( id );
                                nodeRecord.setInUse( inUse );
                                nodeRecord.setNextRel( longFromIntAndMod( nextRel, relModifier ) );
                                nodeRecord.setNextProp( longFromIntAndMod( nextProp, propModifier ) );
                            }
                            id++;
                        } while ( nodeRecord == null && id < maxId );

                        return nodeRecord;
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

}
