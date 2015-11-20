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
package org.neo4j.kernel.recovery;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.rotation.StoreFlusher;

import static org.neo4j.kernel.impl.transaction.log.PhysicalLogFile.tryOpenForVersion;

public class DefaultRecoverySPI implements Recovery.SPI
{
    private final StoreFlusher storeFlusher;
    private final NeoStores neoStores;
    private final Visitor<LogVersionedStoreChannel,Exception> logFileRecoverer;
    private final PhysicalLogFiles logFiles;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final LogVersionRepository logVersionRepository;
    private final PositionToRecoverFrom positionToRecoverFrom;

    public DefaultRecoverySPI(
            StoreFlusher storeFlusher, NeoStores neoStores,
            Visitor<LogVersionedStoreChannel,Exception> logFileRecoverer,
            PhysicalLogFiles logFiles, FileSystemAbstraction fileSystemAbstraction,
            LogVersionRepository logVersionRepository, LatestCheckPointFinder checkPointFinder )
    {
        this.storeFlusher = storeFlusher;
        this.neoStores = neoStores;
        this.logFileRecoverer = logFileRecoverer;
        this.logFiles = logFiles;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.logVersionRepository = logVersionRepository;
        this.positionToRecoverFrom = new PositionToRecoverFrom( checkPointFinder );
    }

    @Override
    public void forceEverything()
    {
        storeFlusher.forceEverything();
    }

    @Override
    public Visitor<LogVersionedStoreChannel,Exception> getRecoverer()
    {
        return logFileRecoverer;
    }

    @Override
    public Iterator<LogVersionedStoreChannel> getLogFiles( final long fromVersion ) throws IOException
    {
        return new Iterator<LogVersionedStoreChannel>()
        {
            private long currentVersion = fromVersion;
            private LogVersionedStoreChannel current =
                    tryOpenForVersion( logFiles, fileSystemAbstraction, currentVersion );

            @Override
            public boolean hasNext()
            {
                return current != null;
            }

            @Override
            public LogVersionedStoreChannel next()
            {
                if ( current == null )
                {
                    throw new NoSuchElementException();
                }

                LogVersionedStoreChannel tmp = current;
                current = tryOpenForVersion( logFiles, fileSystemAbstraction, ++currentVersion );
                return tmp;
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public LogPosition getPositionToRecoverFrom() throws IOException
    {
        return positionToRecoverFrom.apply( logVersionRepository.getCurrentLogVersion() );
    }


    @Override
    public void recoveryRequired()
    {
        // This method will be called before recovery actually starts and so will ensure that
        // each store is aware that recovery will be performed. At this point all the stores have
        // already started btw.
        // Go and read more at {@link CommonAbstractStore#deleteIdGenerator()}
        neoStores.deleteIdGenerators();
    }
}
