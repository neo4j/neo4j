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
package org.neo4j.kernel.impl.core;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.ByteBuffer;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Function;
import org.neo4j.kernel.impl.nioneo.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.nioneo.store.FileLock;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.nioneo.store.StoreFileChannel;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.test.impl.ChannelInputStream;
import org.neo4j.test.impl.ChannelOutputStream;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

public class JumpingFileSystemAbstraction extends LifecycleAdapter implements FileSystemAbstraction
{
    private final int sizePerJump;
    private final EphemeralFileSystemAbstraction actualFileSystem = new EphemeralFileSystemAbstraction();

    public JumpingFileSystemAbstraction( int sizePerJump )
    {
        this.sizePerJump = sizePerJump;
    }
    
    @Override
    public StoreChannel open( File fileName, String mode ) throws IOException
    {
        StoreFileChannel channel = (StoreFileChannel) actualFileSystem.open( fileName, mode );
        if (
                fileName.getName().equals( "neostore.nodestore.db" ) ||
                fileName.getName().equals( "neostore.nodestore.db.labels" ) ||
                fileName.getName().equals( "neostore.relationshipstore.db" ) ||
                fileName.getName().equals( "neostore.propertystore.db" ) ||
                fileName.getName().equals( "neostore.propertystore.db.strings" ) ||
                fileName.getName().equals( "neostore.propertystore.db.arrays" ) )
        {        
            return new JumpingFileChannel( channel, recordSizeFor( fileName ) );
        }
        return channel;
    }
    
    @Override
    public OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException
    {
        return new ChannelOutputStream( open( fileName, "rw" ), append );
    }
    
    @Override
    public InputStream openAsInputStream( File fileName ) throws IOException
    {
        return new ChannelInputStream( open( fileName, "r" ) );
    }

    @Override
    public Reader openAsReader( File fileName, String encoding ) throws IOException
    {
        return new InputStreamReader( openAsInputStream( fileName ), encoding );
    }
    
    @Override
    public Writer openAsWriter( File fileName, String encoding, boolean append ) throws IOException
    {
        return new OutputStreamWriter( openAsOutputStream( fileName, append ), encoding );
    }

    @Override
    public StoreChannel create( File fileName ) throws IOException
    {
        return open( fileName, "rw" );
    }
    
    @Override
    public boolean fileExists( File fileName )
    {
        return actualFileSystem.fileExists( fileName );
    }
    
    @Override
    public long getFileSize( File fileName )
    {
        return actualFileSystem.getFileSize( fileName );
    }
    
    @Override
    public boolean deleteFile( File fileName )
    {
        return actualFileSystem.deleteFile( fileName );
    }
    
    @Override
    public void deleteRecursively( File directory ) throws IOException
    {
        actualFileSystem.deleteRecursively( directory );
    }
    
    @Override
    public boolean mkdir( File fileName )
    {
        return actualFileSystem.mkdir( fileName );
    }
    
    @Override
    public void mkdirs( File fileName )
    {
        actualFileSystem.mkdirs( fileName );
    }
    
    @Override
    public boolean renameFile( File from, File to ) throws IOException
    {
        return actualFileSystem.renameFile( from, to );
    }

    @Override
    public FileLock tryLock( File fileName, StoreChannel channel ) throws IOException
    {
        return actualFileSystem.tryLock( fileName, channel );
    }
    
    @Override
    public File[] listFiles( File directory )
    {
        return actualFileSystem.listFiles( directory );
    }

    @Override
    public File[] listFiles( File directory, FilenameFilter filter )
    {
        return actualFileSystem.listFiles( directory, filter );
    }
    
    @Override
    public boolean isDirectory( File file )
    {
        return actualFileSystem.isDirectory( file );
    }
    
    @Override
    public void moveToDirectory( File file, File toDirectory ) throws IOException
    {
        actualFileSystem.moveToDirectory( file, toDirectory );
    }
    
    @Override
    public void copyFile( File from, File to ) throws IOException
    {
        actualFileSystem.copyFile( from, to );
    }
    
    @Override
    public void copyRecursively( File fromDirectory, File toDirectory ) throws IOException
    {
        actualFileSystem.copyRecursively( fromDirectory, toDirectory );
    }
    
    private int recordSizeFor( File fileName )
    {
        if ( fileName.getName().endsWith( "nodestore.db" ) )
        {
            return NodeStore.RECORD_SIZE;
        }
        else if ( fileName.getName().endsWith( "relationshipstore.db" ) )
        {
            return RelationshipStore.RECORD_SIZE;
        }
        else if ( fileName.getName().endsWith( "propertystore.db.strings" ) ||
                fileName.getName().endsWith( "propertystore.db.arrays" ) )
        {
            return AbstractDynamicStore.getRecordSize( PropertyStore.DEFAULT_DATA_BLOCK_SIZE );
        }
        else if ( fileName.getName().endsWith( "propertystore.db" ) )
        {
            return PropertyStore.RECORD_SIZE;
        }
        else if ( fileName.getName().endsWith( "nodestore.db.labels" ) )
        {
            return Integer.parseInt( GraphDatabaseSettings.label_block_size.getDefaultValue() );
        }
        else if ( fileName.getName().endsWith( "schemastore.db" ) )
        {
            return SchemaStore.getRecordSize( SchemaStore.BLOCK_SIZE );
        }
        throw new IllegalArgumentException( fileName.getPath() );
    }
    
    public class JumpingFileChannel extends StoreFileChannel
    {
        private final int recordSize;
        
        public JumpingFileChannel( StoreFileChannel actual, int recordSize )
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
            long actualRecord = position/recordSize;
            if ( actualRecord < sizePerJump/2 )
            {
                return position;
            }
            else
            {
                long jumpIndex = (actualRecord+sizePerJump)/0x100000000L;
                long diff = actualRecord - jumpIndex * 0x100000000L;
                diff = assertWithinDiff( diff, allowFix );
                long offsettedRecord = jumpIndex*sizePerJump + diff;
                return offsettedRecord*recordSize;
            }
        }
        
        private long translateOutgoing( long offsettedPosition )
        {
            long offsettedRecord = offsettedPosition/recordSize;
            if ( offsettedRecord < sizePerJump/2 )
            {
                return offsettedPosition;
            }
            else
            {
                long jumpIndex = (offsettedRecord-sizePerJump/2) / sizePerJump + 1;
                long diff = ((offsettedRecord-sizePerJump/2) % sizePerJump) - sizePerJump/2;
                assertWithinDiff( diff, false );
                long actualRecord = jumpIndex*0x100000000L - sizePerJump/2 + diff;
                return actualRecord*recordSize;
            }
        }

        private long assertWithinDiff( long diff, boolean allowFix )
        {
            if ( diff < -sizePerJump/2 || diff > sizePerJump/2 )
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

        @Override
        public int write( ByteBuffer src, long position ) throws IOException
        {
            return super.write( src, translateIncoming( position ) );
        }
    }

    @Override
    public <K extends ThirdPartyFileSystem> K getOrCreateThirdPartyFileSystem(
            Class<K> clazz, Function<Class<K>, K> creator )
    {
        return actualFileSystem.getOrCreateThirdPartyFileSystem( clazz, creator );
    }

    @Override
    public void shutdown()
    {
        actualFileSystem.shutdown();
    }
}
