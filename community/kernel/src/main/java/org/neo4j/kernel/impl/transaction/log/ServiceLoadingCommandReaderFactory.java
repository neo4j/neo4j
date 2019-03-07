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
package org.neo4j.kernel.impl.transaction.log;

import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion;
import org.neo4j.service.Services;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.util.Preconditions.checkState;

public class ServiceLoadingCommandReaderFactory implements CommandReaderFactory
{
    private final IntObjectMap<CommandReader> readersByFormatId;

    public ServiceLoadingCommandReaderFactory()
    {
        this( Services.loadAll( CommandReader.class ) );
    }

    @VisibleForTesting
    ServiceLoadingCommandReaderFactory( Iterable<CommandReader> readers )
    {
        final MutableIntObjectMap<CommandReader> readerByFormatId = new IntObjectHashMap<>();
        for ( CommandReader reader : readers )
        {
            final CommandReader prev = readerByFormatId.put( reader.getFormatId(), reader );
            checkState( prev == null, "Command format %d support is declared by multiple readers: %s and %s", reader.getFormatId(), reader, prev );
        }
        this.readersByFormatId = readerByFormatId.toImmutable();

        verifyLegacyVersionsSupport();
    }

    private void verifyLegacyVersionsSupport()
    {
        LogEntryVersion[] versions = LogEntryVersion.values();
        for ( LogEntryVersion version : versions )
        {
            if ( readersByFormatId.get( version.byteCode() ) == null )
            {
                throw new AssertionError( "Version " + version + " not handled" );
            }
        }
    }

    @Override
    public CommandReader get( int formatId )
    {
        final CommandReader reader = readersByFormatId.get( formatId );
        if ( reader == null )
        {
            throw new IllegalArgumentException( "Unsupported command format [id=" + formatId + "]" );
        }
        return reader;
    }
}
