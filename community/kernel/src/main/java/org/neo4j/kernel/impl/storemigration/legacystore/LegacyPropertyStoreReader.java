/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.text.MessageFormat;

import org.neo4j.kernel.impl.nioneo.store.Buffer;
import org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore;
import org.neo4j.kernel.impl.nioneo.store.OperationType;
import org.neo4j.kernel.impl.nioneo.store.PersistenceWindow;
import org.neo4j.kernel.impl.nioneo.store.PersistenceWindowPool;
import org.neo4j.kernel.impl.nioneo.store.Record;

public class LegacyPropertyStoreReader
{
    public static final String FROM_VERSION = "PropertyStore v0.9.9";
    public static final int RECORD_LENGTH = 25;
    private PersistenceWindowPool windowPool;
    private final FileChannel fileChannel;

    public LegacyPropertyStoreReader( String fileName ) throws FileNotFoundException
    {
        fileChannel = new RandomAccessFile( fileName, "r" ).getChannel();
        windowPool = new PersistenceWindowPool( fileName,
                RECORD_LENGTH, fileChannel, CommonAbstractStore.calculateMappedMemory( null, fileName ),
                true, true );
    }

    public LegacyPropertyRecord readPropertyRecord( long id ) throws IOException
    {
        PersistenceWindow persistenceWindow = windowPool.acquire( id, OperationType.READ );
        try
        {
            Buffer buffer = persistenceWindow.getOffsettedBuffer( id );

            // [    ,   x] in use
            // [xxxx,    ] high prev prop bits
            long inUseByte = buffer.get();

            boolean inUse = (inUseByte & 0x1) == Record.IN_USE.intValue();
            if ( !inUse )
            {
                throw new IllegalArgumentException( MessageFormat.format( "Record {0} not in use", id ) );
            }
            LegacyPropertyRecord record = new LegacyPropertyRecord( id );

            // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
            // [    ,    ][    ,xxxx][    ,    ][    ,    ] high next prop bits
            long typeInt = buffer.getInt();

            record.setType( getEnumType( (int) typeInt & 0xFFFF ) );
            record.setInUse( true );
            record.setKeyIndexId( buffer.getInt() );
            record.setPropBlock( buffer.getLong() );

            long prevProp = buffer.getUnsignedInt();
            long prevModifier = (inUseByte & 0xF0L) << 28;
            long nextProp = buffer.getUnsignedInt();
            long nextModifier = (typeInt & 0xF0000L) << 16;

            record.setPrevProp( LegacyStore.longFromIntAndMod( prevProp, prevModifier ) );
            record.setNextProp( LegacyStore.longFromIntAndMod( nextProp, nextModifier ) );

            return record;
        }
        finally
        {
            windowPool.release( persistenceWindow );
        }
    }

    private LegacyPropertyType getEnumType( int type )
    {
        return LegacyPropertyType.getPropertyType( type, false );
    }

    public void close() throws IOException
    {
        fileChannel.close();
    }
}
