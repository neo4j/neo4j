/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * An abstract representation of a store. A store is a file that contains
 * records. Each record has a fixed size (<CODE>getRecordSize()</CODE>) so
 * the position for a record can be calculated by
 * <CODE>id * getRecordSize()</CODE>.
 * <p>
 * A store has an {@link IdGenerator} managing the records that are free or in
 * use.
 */
public abstract class AbstractStore extends CommonAbstractStore
{
    public static abstract class Configuration extends CommonAbstractStore.Configuration
    {
    }

    private final Config conf;

    /**
     * Returns the fixed size of each record in this store.
     *
     * @return The record size
     */
    public abstract int getRecordSize();

    public AbstractStore( File fileName, Config conf, IdType idType,
                          IdGeneratorFactory idGeneratorFactory, WindowPoolFactory windowPoolFactory,
                          FileSystemAbstraction fileSystemAbstraction, StringLogger stringLogger,
                          StoreVersionMismatchHandler versionMismatchHandler )
    {
        super( fileName, conf, idType, idGeneratorFactory, windowPoolFactory, fileSystemAbstraction, stringLogger,
                versionMismatchHandler );
        this.conf = conf;
    }

    @Override
    protected int getEffectiveRecordSize()
    {
        return getRecordSize();
    }

    @Override
    protected void readAndVerifyBlockSize() throws IOException
    {
        // record size is fixed for non-dynamic stores, so nothing to do here
    }

    @Override
    protected void verifyFileSizeAndTruncate() throws IOException
    {
        int expectedVersionLength = UTF8.encode( buildTypeDescriptorAndVersion( getTypeDescriptor() ) ).length;
        long fileSize = getFileChannel().size();
        if ( getRecordSize() != 0
             && (fileSize - expectedVersionLength) % getRecordSize() != 0 && !isReadOnly() )
        {
            setStoreNotOk( new IllegalStateException(
                    "Misaligned file size " + fileSize + " for " + this + ", expected version length:" +
                    expectedVersionLength ) );
        }
        if ( getStoreOk() && !isReadOnly() )
        {
            getFileChannel().truncate( fileSize - expectedVersionLength );
        }
    }

    protected boolean isRecordInUse( ByteBuffer buffer )
    {
        byte inUse = buffer.get();
        return (inUse & 0x1) == Record.IN_USE.byteValue();
    }

    public abstract List<WindowPoolStats> getAllWindowPoolStats();

    public void logAllWindowPoolStats( StringLogger.LineLogger logger )
    {
        logger.logLine( getWindowPoolStats().toString() );
    }
}
