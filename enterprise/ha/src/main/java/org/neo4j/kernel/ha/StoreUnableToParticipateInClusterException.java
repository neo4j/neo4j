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
package org.neo4j.kernel.ha;

/**
 * Exception indicating that a store is not fit for participating in a particular cluster. It might have diverged, be
 * to old or otherwise unfit for a cluster. This does not mean however that it is corrupt or not in some way suitable
 * for standalone use.
 */
public class StoreUnableToParticipateInClusterException extends IllegalStateException
{
    public StoreUnableToParticipateInClusterException()
    {
        super();
    }

    public StoreUnableToParticipateInClusterException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public StoreUnableToParticipateInClusterException( String message )
    {
        super( message );
    }

    public StoreUnableToParticipateInClusterException( Throwable cause )
    {
        super( cause );
    }
}
