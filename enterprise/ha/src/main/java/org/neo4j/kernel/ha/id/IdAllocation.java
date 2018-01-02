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
