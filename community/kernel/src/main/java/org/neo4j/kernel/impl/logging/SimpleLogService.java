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
package org.neo4j.kernel.impl.logging;

import org.neo4j.logging.DuplicatingLogProvider;
import org.neo4j.logging.LogProvider;

public class SimpleLogService extends AbstractLogService
{
    private final LogProvider userLogProvider;
    private final LogProvider internalLogProvider;

    /**
     * Create log service where both: user and internal log provider use the same {@link LogProvider} as a provider.
     * Should be used when user and internal are backed by same log provider.
     * @param commonLogProvider log provider
     */
    public SimpleLogService( LogProvider commonLogProvider )
    {
        this.userLogProvider = commonLogProvider;
        this.internalLogProvider = commonLogProvider;
    }

    /**
     * Create log service with different user and internal log providers.
     * User logs will be duplicated to internal logs as well.
     * Should be used when user and internal are backed by different log providers.
     * @param userLogProvider user log provider
     * @param internalLogProvider internal log provider
     */
    public SimpleLogService( LogProvider userLogProvider, LogProvider internalLogProvider )
    {
        this.userLogProvider = new DuplicatingLogProvider( userLogProvider, internalLogProvider );
        this.internalLogProvider = internalLogProvider;
    }

    @Override
    public LogProvider getUserLogProvider()
    {
        return this.userLogProvider;
    }

    @Override
    public LogProvider getInternalLogProvider()
    {
        return this.internalLogProvider;
    }
}
