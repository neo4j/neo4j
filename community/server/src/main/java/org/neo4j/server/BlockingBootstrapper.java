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
package org.neo4j.server;

import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

public class BlockingBootstrapper implements Bootstrapper
{
    private final Bootstrapper wrapped;
    private final CountDownLatch latch;

    public BlockingBootstrapper( Bootstrapper wrapped )
    {
        this.wrapped = wrapped;
        this.latch = new CountDownLatch( 1 );
    }

    @Override
    public final int start( File homeDir, Optional<File> configFile, Map<String, String> configOverrides )
    {
        int status = wrapped.start( homeDir, configFile, configOverrides );
        if ( status != ServerBootstrapper.OK )
        {
            return status;
        }

        try
        {
            latch.await();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }

        return wrapped.stop();
    }

    @Override
    public int stop()
    {
        latch.countDown();
        return 0;
    }
}
