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

package org.neo4j.kernel.logging;

import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Implements the old-style logging with just one logger regardless of name.
 */
public class ClassicLoggingService
    extends LifecycleAdapter
    implements Logging
{
    private Config config;
    protected StringLogger stringLogger;

    public ClassicLoggingService(Config config)
    {
        this.config = config;
        stringLogger = StringLogger.logger( config.get( InternalAbstractGraphDatabase.Configuration.store_dir ) );
    }

    @Override
    public void init()
        throws Throwable
    {
    }

    @Override
    public void shutdown()
        throws Throwable
    {
        stringLogger.close();
    }

    @Override
    public StringLogger getLogger( String name )
    {
        return stringLogger;
    }
}
