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

package org.neo4j.test.impl;

import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.Logging;

/**
 * Implements logging with just one logger regardless of name. The internal {@link StringLogger}
 * wraps around a UTF-8 {@code Writer}, which in turn wraps a {@code FileChannel}.
 */
public class FileChannelLoggingService
    extends LifecycleAdapter
    implements Logging
{
    private final StringLogger stringLogger;

    public FileChannelLoggingService(FileChannel fileChannel) {
        Writer writer = Channels.newWriter(fileChannel, "UTF-8");
        stringLogger = StringLogger.wrap(writer);
    }

    @Override
    public void shutdown() {
        stringLogger.close();
    }

    @Override
    public StringLogger getLogger( String name )
    {
        return stringLogger;
    }
}
