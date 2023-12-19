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
package org.neo4j.kernel.enterprise.builtinprocs;

import org.neo4j.kernel.impl.locking.ActiveLock;

public class ActiveLocksResult
{
    public final String mode;
    public final String resourceType;
    public final long resourceId;

    public ActiveLocksResult( ActiveLock lock )
    {
        this.mode = lock.mode();
        this.resourceType = lock.resourceType().name();
        this.resourceId = lock.resourceId();
    }
}
