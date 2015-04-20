/*
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
package org.neo4j.kernel.api.index;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongObjectVisitor;
import org.neo4j.graphdb.Resource;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Meta data about a {@link SchemaIndexProvider}.
 * Its general format is a simplified version of {@link NeoStore} where if can read and write records
 * of type long. Each record has one byte of "in use" and 8 bytes of long data.
 */
public class ProviderMeta extends LifecycleAdapter
{
    public static final String DEFAULT_NAME = "meta";

    public static final long ID_VERSION = 0;

    static final int RECORD_SIZE = 9;

    private final FileSystemAbstraction fs;
    private final File file;
    private final ByteBuffer buffer = ByteBuffer.allocate( RECORD_SIZE );
    private StoreChannel channel;

    // Snapshot state
    private int snapshotCount;
    private final PrimitiveLongObjectMap<Record> snapshotRecords = Primitive.longObjectMap();

    public ProviderMeta( FileSystemAbstraction fs, File file )
    {
        this.fs = fs;
        this.file = file;
    }

    @Override
    public synchronized void start() throws IOException
    {
        channel = fs.open( file, "rw" );
    }

    @Override
    public synchronized void stop() throws IOException
    {
        updatePendingSnapshotChanges();
        channel.close();
        channel = null;
    }

    public synchronized Record getRecord( long id ) throws InvalidRecordException
    {
        if ( snapshotCount > 0 )
        {
            Record record = snapshotRecords.get( id );
            if ( record != null )
            {
                return record;
            }
        }

        try
        {
            channel.position( id*RECORD_SIZE );
            buffer.clear();
            int bytesRead = channel.read( buffer );
            buffer.flip();
            if ( bytesRead == -1 )
            {   // We're past EOF
                throw new InvalidRecordException( "No such record " + id );
            }

            if ( bytesRead != RECORD_SIZE )
            {   // We're looking at a corrupted file, dunno yet how to deal with it TODO
                throw new Error( "Urgh" );
            }

            byte inUseByte = buffer.get();
            boolean inUse = inUseByte == 1;
            if ( !inUse )
            {
                throw new InvalidRecordException( "Record " + id + " not in use" );
            }

            long value = buffer.getLong();
            Record record = new Record( id, value );
            record.setInUse( true );
            return record;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Error reading channel", e );
        }
    }

    public synchronized void updateRecord( Record record )
    {
        if ( snapshotCount > 0 )
        {
            snapshotRecords.put( record.getLongId(), record );
            return;
        }

        buffer.clear();
        buffer.put( (byte) (record.inUse() ? 1 : 0) );
        buffer.putLong( record.getValue() );
        buffer.flip();

        try
        {
            channel.position( record.getLongId()*RECORD_SIZE );
            channel.write( buffer );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Error writing channel", e );
        }
    }

    public static class Record extends AbstractBaseRecord
    {
        private final long id;
        private long value;

        public Record( long id, long value )
        {
            this.id = id;
            this.value = value;
            setInUse( true );
        }

        @Override
        public long getLongId()
        {
            return id;
        }

        public void setValue( long value )
        {
            this.value = value;
        }

        public long getValue()
        {
            return value;
        }
    }

    public void force()
    {
        try
        {
            channel.force( true );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Error forcing channel", e );
        }
    }

    public class Snapshot implements Resource
    {
        private boolean closed;

        public File getFile()
        {
            return file;
        }

        @Override
        public void close()
        {
            if ( !closed )
            {
                releaseSnapshot( this );
                closed = true;
            }
        }
    }

    public synchronized Snapshot snapshot()
    {
        snapshotCount++;
        if ( snapshotCount == 1 )
        {   // We now enter "snapshot mode", keep the file steady
            force();
        }
        return new Snapshot();
    }

    private synchronized void releaseSnapshot( Snapshot snapshot )
    {
        snapshotCount--;
        if ( snapshotCount == 0 )
        {   // We now exit "snapshot mode", flush all pending updates to the channel
            updatePendingSnapshotChanges();
        }
    }

    private void updatePendingSnapshotChanges()
    {
        snapshotRecords.visitEntries( new PrimitiveLongObjectVisitor<ProviderMeta.Record, RuntimeException>()
        {
            @Override
            public boolean visited( long key, Record value )
            {
                updateRecord( value );
                return false;
            }
        } );
    }
}
