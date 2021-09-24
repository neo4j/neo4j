/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.logging.internal;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;

public final class NullLogService implements LogService
{
    private static final NullLogService INSTANCE = new NullLogService();
    private static final NullLogProvider NULL_LOG_PROVIDER = NullLogProvider.getInstance();
    private static final NullLog NULL_LOG = NullLog.getInstance();

    private NullLogService()
    {
    }

    public static NullLogService getInstance()
    {
        return INSTANCE;
    }

    @Override
    public LogProvider getUserLogProvider()
    {
        return NULL_LOG_PROVIDER;
    }

    @Override
    public Log getUserLog( Class<?> loggingClass )
    {
        return NULL_LOG;
    }

    @Override
    public LogProvider getInternalLogProvider()
    {
        return NULL_LOG_PROVIDER;
    }

    @Override
    public Log getInternalLog( Class<?> loggingClass )
    {
        return NULL_LOG;
    }
}
