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
package org.neo4j.kernel.logging;

import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class SingleLoggingService extends LifecycleAdapter implements Logging
{
    private final StringLogger logger;
    private final ConsoleLogger consoleLogger;

    public SingleLoggingService( StringLogger logger )
    {
        this.logger = logger;
        this.consoleLogger = new ConsoleLogger( this.logger );
    }
    
    @Override
    public StringLogger getMessagesLog( Class loggingClass )
    {
        return logger;
    }

    @Override
    public ConsoleLogger getConsoleLog( Class loggingClass )
    {
        return consoleLogger;
    }

    @Override
    public void shutdown() throws Throwable
    {
        logger.close();
    }
}
