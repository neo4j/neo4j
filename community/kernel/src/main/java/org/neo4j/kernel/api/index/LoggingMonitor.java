/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.api.index;

import java.util.Map;
import java.util.StringJoiner;

import org.neo4j.kernel.api.schema.index.StoreIndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.logging.Log;

import static java.lang.String.format;

public class LoggingMonitor implements IndexProvider.Monitor
{
    private final Log log;

    public LoggingMonitor( Log log )
    {
        this.log = log;
    }

    @Override
    public void failedToOpenIndex( StoreIndexDescriptor descriptor, String action, Exception cause )
    {
        log.error( "Failed to open index:" + descriptor.getId() + ". " + action, cause );
    }

    @Override
    public void recoveryCompleted( IndexDescriptor schemaIndexDescriptor, String indexFile, Map<String,Object> data )
    {
        StringJoiner joiner = new StringJoiner( ", ", "Schema index recovery completed: ", "" );
        joiner.add( "descriptor=" + schemaIndexDescriptor );
        joiner.add( "file=" + indexFile );
        data.forEach( ( key, value ) -> joiner.add( format( "%s=%s", key, value ) ) );
        log.info( joiner.toString() );
    }
}
