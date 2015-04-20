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
package org.neo4j.kernel.impl.transaction.log;

import java.io.File;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.record.NeoStoreUtil;

public class ReadOnlyLogVersionRepository implements LogVersionRepository
{
    private final long logVersion;
    private volatile boolean incrementVersionCalled;

    public ReadOnlyLogVersionRepository( FileSystemAbstraction fs, File storeDir )
    {
        this.logVersion = NeoStoreUtil.neoStoreExists( fs, storeDir ) ?
                new NeoStoreUtil( storeDir, fs ).getLogVersion() : 0;
    }

    @Override
    public long getCurrentLogVersion()
    {
        // We can expect a call to this during shutting down, if we have a LogFile using us.
        // So it's sort of OK.
        if ( incrementVersionCalled )
        {
            throw new IllegalStateException( "Read-only log version repository has observed a call to " +
                    "incrementVersion, which indicates that it's been shut down" );
        }
        return logVersion;
    }

    @Override
    public long incrementAndGetVersion()
    {   // We can expect a call to this during shutting down, if we have a LogFile using us.
        // So it's sort of OK.
        if ( incrementVersionCalled )
        {
            throw new IllegalStateException( "Read-only log version repository only allows " +
                    "to call incrementVersion once, during shutdown" );
        }
        incrementVersionCalled = true;
        return logVersion;
    }
}
