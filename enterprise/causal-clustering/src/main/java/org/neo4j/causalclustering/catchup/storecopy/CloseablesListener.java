/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.io.IOUtils;

class CloseablesListener implements AutoCloseable, GenericFutureListener<Future<Void>>
{
    private final List<AutoCloseable> closeables = new ArrayList<>();

    <T extends AutoCloseable> T add( T closeable )
    {
        if ( closeable == null )
        {
            throw new IllegalArgumentException( "closeable cannot be null!" );
        }
        closeables.add( closeable );
        return closeable;
    }

    @Override
    public void close()
    {
        IOUtils.closeAll( RuntimeException.class, closeables.toArray( new AutoCloseable[closeables.size()] ) );
    }

    @Override
    public void operationComplete( Future<Void> future )
    {
        close();
    }
}
