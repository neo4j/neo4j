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
package org.neo4j.causalclustering.core.replication.session;

/** Context for operation. Used for acquirement and release. */
public class OperationContext
{
    private final GlobalSession globalSession;
    private final LocalOperationId localOperationId;

    private final LocalSession localSession;

    public OperationContext( GlobalSession globalSession, LocalOperationId localOperationId, LocalSession localSession )
    {
        this.globalSession = globalSession;
        this.localOperationId = localOperationId;
        this.localSession = localSession;
    }

    public GlobalSession globalSession()
    {
        return globalSession;
    }

    public LocalOperationId localOperationId()
    {
        return localOperationId;
    }

    protected LocalSession localSession()
    {
        return localSession;
    }

    @Override
    public String toString()
    {
        return "OperationContext{" +
               "globalSession=" + globalSession +
               ", localOperationId=" + localOperationId +
               '}';
    }
}
