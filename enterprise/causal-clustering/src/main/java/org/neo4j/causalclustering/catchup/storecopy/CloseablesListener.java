/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
