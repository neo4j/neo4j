/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.core;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.DelegatingFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.StoreFileChannel;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.format.standard.DynamicRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.NodeRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.PropertyRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.RelationshipGroupRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.RelationshipRecordFormat;
import org.neo4j.test.impl.ChannelInputStream;
import org.neo4j.test.impl.ChannelOutputStream;

public class JumpingFileSystemAbstraction extends DelegatingFileSystemAbstraction
{
    private final int sizePerJump;

    public JumpingFileSystemAbstraction( int sizePerJump )
    {
        this( new EphemeralFileSystemAbstraction(), sizePerJump );
    }

    private JumpingFileSystemAbstraction( EphemeralFileSystemAbstraction ephemeralFileSystem, int sizePerJump )
    {
        super( ephemeralFileSystem );
        this.sizePerJump = sizePerJump;
    }

    @Override
    public StoreChannel open( File fileName, OpenMode openMode ) throws IOException
    {
        StoreFileChannel channel = (StoreFileChannel) super.open( fileName, openMode );
        if (
                fileName.getName().equals( "neostore.nodestore.db" ) ||
                fileName.getName().equals( "neostore.nodestore.db.labels" ) ||
                fileName.getName().equals( "neostore.relationshipstore.db" ) ||
                fileName.getName().equals( "neostore.propertystore.db" ) ||
                fileName.getName().equals( "neostore.propertystore.db.strings" ) ||
                fileName.getName().equals( "neostore.propertystore.db.arrays" ) ||
                fileName.getName().equals( "neostore.relationshipgroupstore.db" ) )
        {
            return new JumpingFileChannel( channel, recordSizeFor( fileName ) );
        }
        return channel;
    }

    @Override
    public OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException
    {
        return new ChannelOutputStream( open( fileName, OpenMode.READ_WRITE ), append );
    }

    @Override
    public InputStream openAsInputStream( File fileName ) throws IOException
    {
        return new ChannelInputStream( open( fileName, OpenMode.READ ) );
    }

    @Override
    public Reader openAsReader( File fileName, Charset charset ) throws IOException
    {
        return new InputStreamReader( openAsInputStream( fileName ), charset );
    }

    @Override
    public Writer openAsWriter( File fileName, Charset charset, boolean append ) throws IOException
    {
        return new OutputStreamWriter( openAsOutputStream( fileName, append ), charset );
    }

    @Override
    public StoreChannel create( File fileName ) throws IOException
    {
        return open( fileName, OpenMode.READ_WRITE );
    }

    public static int getRecordSize( int dataSize )
    {
        return dataSize + DynamicRecordFormat.RECORD_HEADER_SIZE;
    }

    private int recordSizeFor( File fileName )
    {
        if ( fileName.getName().endsWith( "nodestore.db" ) )
        {
            return NodeRecordFormat.RECORD_SIZE;
        }
        else if ( fileName.getName().endsWith( "relationshipstore.db" ) )
        {
            return RelationshipRecordFormat.RECORD_SIZE;
        }
        else if ( fileName.getName().endsWith( "propertystore.db.strings" ) ||
                fileName.getName().endsWith( "propertystore.db.arrays" ) )
        {
            return getRecordSize( PropertyRecordFormat.DEFAULT_DATA_BLOCK_SIZE );
        }
        else if ( fileName.getName().endsWith( "propertystore.db" ) )
        {
            return PropertyRecordFormat.RECORD_SIZE;
        }
        else if ( fileName.getName().endsWith( "nodestore.db.labels" ) )
        {
            return Integer.parseInt( GraphDatabaseSettings.label_block_size.getDefaultValue() ) +
                    DynamicRecordFormat.RECORD_HEADER_SIZE;
        }
        else if ( fileName.getName().endsWith( "schemastore.db" ) )
        {
            return getRecordSize( SchemaStore.BLOCK_SIZE );
        }
        else if ( fileName.getName().endsWith( "relationshipgroupstore.db" ) )
        {
            return getRecordSize( RelationshipGroupRecordFormat.RECORD_SIZE );
        }
        throw new IllegalArgumentException( fileName.getPath() );
    }

    public class JumpingFileChannel extends StoreFileChannel
    {
        private final int recordSize;

        JumpingFileChannel( StoreFileChannel actual, int recordSize )
        {
            super( actual );
            this.recordSize = recordSize;
        }

        private long translateIncoming( long position )
        {
            return translateIncoming( position, false );
        }

        private long translateIncoming( long position, boolean allowFix )
        {
            long actualRecord = position / recordSize;
            if ( actualRecord < sizePerJump / 2 )
            {
                return position;
            }
            else
            {
                long jumpIndex = (actualRecord + sizePerJump) / 0x100000000L;
                long diff = actualRecord - jumpIndex * 0x100000000L;
                diff = assertWithinDiff( diff, allowFix );
                long offsettedRecord = jumpIndex * sizePerJump + diff;
                return offsettedRecord * recordSize;
            }
        }

        private long translateOutgoing( long offsettedPosition )
        {
            long offsettedRecord = offsettedPosition / recordSize;
            if ( offsettedRecord < sizePerJump / 2 )
            {
                return offsettedPosition;
            }
            else
            {
                long jumpIndex = (offsettedRecord - sizePerJump / 2) / sizePerJump + 1;
                long diff = ((offsettedRecord - sizePerJump / 2) % sizePerJump) - sizePerJump / 2;
                assertWithinDiff( diff, false );
                long actualRecord = jumpIndex * 0x100000000L - sizePerJump / 2 + diff;
                return actualRecord * recordSize;
            }
        }

        private long assertWithinDiff( long diff, boolean allowFix )
        {
            if ( diff < -sizePerJump / 2 || diff > sizePerJump / 2 )
            {
                if ( allowFix )
                {
                    // This is needed for shutdown() to work, PropertyStore
                    // gives an invalid offset for truncate.
                    if ( diff < -sizePerJump / 2 )
                    {
                        return -sizePerJump / 2;
                    }
                    else
                    {
                        return sizePerJump / 2;
                    }
                }
                throw new IllegalArgumentException( "" + diff );
            }
            return diff;
        }

        @Override
        public long position() throws IOException
        {
            return translateOutgoing( super.position() );
        }

        @Override
        public JumpingFileChannel position( long newPosition ) throws IOException
        {
            super.position( translateIncoming( newPosition ) );
            return this;
        }

        @Override
        public long size() throws IOException
        {
            return translateOutgoing( super.size() );
        }

        @Override
        public JumpingFileChannel truncate( long size ) throws IOException
        {
            super.truncate( translateIncoming( size, true ) );
            return this;
        }

        @Override
        public int read( ByteBuffer dst, long position ) throws IOException
        {
            return super.read( dst, translateIncoming( position ) );
        }
    }
}
