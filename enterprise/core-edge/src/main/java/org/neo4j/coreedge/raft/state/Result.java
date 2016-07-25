/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.raft.state;

import java.util.concurrent.CompletableFuture;

public class Result
{
    private final Exception exception;
    private final Object result;

    private Result( Exception exception )
    {
        this.exception = exception;
        this.result = null;
    }

    private Result( Object result )
    {
        this.result = result;
        this.exception = null;
    }

    public static Result of( Object result )
    {
        return new Result( result );
    }

    public static Result of( Exception exception )
    {
        return new Result( exception );
    }

    public Object consume() throws Exception
    {
        if( exception != null )
        {
            throw exception;
        }
        else
        {
            return result;
        }
    }

    public CompletableFuture<Object> apply( CompletableFuture<Object> future )
    {
        if( exception != null )
        {
            future.completeExceptionally( exception );
        }
        else
        {
            future.complete( result );
        }

        return future;
    }

    @Override
    public String toString()
    {
        return "Result{" +
               "exception=" + exception +
               ", result=" + result +
               '}';
    }
}
