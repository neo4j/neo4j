/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.recordstorage;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat;
import org.neo4j.storageengine.api.LogVersionRepository;

import static org.neo4j.kernel.impl.store.MetaDataStore.Position.CHECKPOINT_LOG_VERSION;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LOG_VERSION;

public class ReadOnlyLogVersionRepository implements LogVersionRepository
{
    private static final int NOT_EXISTING_VERSION = 0;
    private final FixedLogVersion logVersion;
    private final FixedLogVersion checkpointLogVersion;

    public ReadOnlyLogVersionRepository( PageCache pageCache, DatabaseLayout databaseLayout, PageCursorTracer cursorTracer ) throws IOException
    {
        this.logVersion = new FixedLogVersion( readLogVersion( pageCache, databaseLayout.metadataStore(), cursorTracer, LOG_VERSION ) );
        this.checkpointLogVersion = new FixedLogVersion( readLogVersion( pageCache, databaseLayout.metadataStore(), cursorTracer, CHECKPOINT_LOG_VERSION ) );
    }

    @Override
    public long getCurrentLogVersion()
    {
        return getCurrentVersion( logVersion );
    }

    @Override
    public void setCurrentLogVersion( long version, PageCursorTracer cursorTracer )
    {
        setCurrentVersionAttempt();
    }

    @Override
    public long incrementAndGetVersion( PageCursorTracer cursorTracer )
    {
        return incrementAndGetVersion( logVersion );
    }

    @Override
    public long getCheckpointLogVersion()
    {
        return getCurrentVersion( checkpointLogVersion );
    }

    @Override
    public void setCheckpointLogVersion( long version, PageCursorTracer cursorTracer )
    {
        setCurrentVersionAttempt();
    }

    @Override
    public long incrementAndGetCheckpointLogVersion( PageCursorTracer cursorTracer )
    {
        return incrementAndGetVersion( checkpointLogVersion );
    }

    private long getCurrentVersion( FixedLogVersion version )
    {
        // We can expect a call to this during shutting down, if we have a LogFile using us.
        // So it's sort of OK.
        if ( version.isIncrementAttempted() )
        {
            throw new IllegalStateException( "Read-only log version repository has observed a call to " +
                    "incrementVersion, which indicates that it's been shut down" );
        }
        return version.getValue();
    }

    private void setCurrentVersionAttempt()
    {
        throw new UnsupportedOperationException( "Can't set current log version in read only version repository." );
    }

    private long incrementAndGetVersion( FixedLogVersion version )
    {   // We can expect a call to this during shutting down, if we have a LogFile using us.
        // So it's sort of OK.
        if ( version.isIncrementAttempted() )
        {
            throw new IllegalStateException( "Read-only log version repository only allows " +
                    "to call incrementVersion once, during shutdown" );
        }
        version.setIncrementAttempt();
        return version.getValue();
    }

    private static long readLogVersion( PageCache pageCache, Path neoStore, PageCursorTracer cursorTracer, MetaDataStore.Position position ) throws IOException
    {
        try
        {
            long logVersion = MetaDataStore.getRecord( pageCache, neoStore, position, cursorTracer );
            return logVersion != MetaDataRecordFormat.FIELD_NOT_PRESENT ? logVersion : NOT_EXISTING_VERSION;
        }
        catch ( NoSuchFileException ignore )
        {
            return NOT_EXISTING_VERSION;
        }
    }

    private static class FixedLogVersion
    {
        private boolean incrementAttempt;
        private final long value;

        FixedLogVersion( long value )
        {
            this.value = value;
        }

        boolean isIncrementAttempted()
        {
            return incrementAttempt;
        }

        long getValue()
        {
            return value;
        }

        void setIncrementAttempt()
        {
            this.incrementAttempt = true;
        }
    }
}
