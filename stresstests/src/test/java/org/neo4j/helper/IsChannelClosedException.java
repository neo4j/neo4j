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
package org.neo4j.helper;

import java.nio.channels.ClosedChannelException;
import java.util.function.Predicate;

import org.neo4j.com.ComException;

public class IsChannelClosedException implements Predicate<Throwable>
{
    @Override
    public boolean test( Throwable e )
    {
        if ( e == null )
        {
            return false;
        }

        if ( e instanceof ClosedChannelException )
        {
            return true;
        }

        if ( e instanceof ComException && e.getMessage() != null &&
                e.getMessage().startsWith( "Channel has been closed" ) )
        {
            return true;
        }

        return test( e.getCause() );
    }
}
