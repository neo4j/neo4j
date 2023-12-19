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
package org.neo4j.causalclustering.core.state;

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
        if ( exception != null )
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
        if ( exception != null )
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
