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
package org.neo4j.kernel.ha.id;

import org.neo4j.kernel.impl.store.id.IdRange;

public final class IdAllocation
{
    private final IdRange idRange;
    private final long highestIdInUse;
    private final long defragCount;

    public IdAllocation( IdRange idRange, long highestIdInUse, long defragCount )
    {
        this.idRange = idRange;
        this.highestIdInUse = highestIdInUse;
        this.defragCount = defragCount;
    }

    public long getHighestIdInUse()
    {
        return highestIdInUse;
    }

    public long getDefragCount()
    {
        return defragCount;
    }

    public IdRange getIdRange()
    {
        return this.idRange;
    }

    @Override
    public String toString()
    {
        return "IdAllocation[" + idRange + ", " + highestIdInUse + ", " + defragCount + "]";
    }
}
