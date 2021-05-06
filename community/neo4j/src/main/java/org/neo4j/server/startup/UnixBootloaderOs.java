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
package org.neo4j.server.startup;

import org.apache.commons.lang3.StringUtils;

class UnixBootloaderOs extends AbstractUnixBootloaderOs
{
    private static final int MIN_ALLOWED_OPEN_FILES = 40000;

    UnixBootloaderOs( BootloaderContext ctx )
    {
        super( ctx );
    }

    private static int getFileHandleLimit()
    {
        try
        {
            String result = executeCommand( new String[]{"bash", "-c", "ulimit -n"} );
            return StringUtils.isNumeric( result ) ? Integer.parseInt( result ) : Integer.MAX_VALUE;
        }
        catch ( RuntimeException e )
        { //Ignore this check if it is not available
        }
        return Integer.MAX_VALUE;
    }

    @Override
    long start()
    {
        checkLimits();
        return super.start();
    }

    @Override
    long console() throws BootFailureException
    {
        checkLimits();
        return super.console();
    }

    private void checkLimits()
    {
        int limit = getFileHandleLimit();
        if ( limit < MIN_ALLOWED_OPEN_FILES )
        {
            ctx.err.printf( "WARNING: Max %s open files allowed, minimum of %s recommended. See the Neo4j manual.%n", limit, MIN_ALLOWED_OPEN_FILES );
        }
    }
}
