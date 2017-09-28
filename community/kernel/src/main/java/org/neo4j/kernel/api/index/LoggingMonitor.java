/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.Map;

import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.logging.Log;

import static java.lang.String.format;

public class LoggingMonitor implements SchemaIndexProvider.Monitor
{
    private final Log log;

    public LoggingMonitor( Log log )
    {
        this.log = log;
    }

    @Override
    public void failedToOpenIndex( long indexId, IndexDescriptor indexDescriptor, String action, Exception cause )
    {
        log.error( "Failed to open index:" + indexId + ". " + action, cause );
    }

    @Override
    public void recoveryCompleted( long indexId, IndexDescriptor indexDescriptor, Map<String,Object> data )
    {
        StringBuilder builder =
                new StringBuilder(
                        "Schema index recovery completed: indexId: " + indexId + " descriptor: " + indexDescriptor.toString() );
        data.forEach( ( key, value ) -> builder.append( format( " %s: %s", key, value ) ) );
        log.info( builder.toString() );
    }
}
