/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store.stats;

import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;

public class IdBasedStoreEntityCounters implements StoreEntityCounters
{
    private final IdGeneratorFactory idGeneratorFactory;

    public IdBasedStoreEntityCounters( IdGeneratorFactory idGeneratorFactory )
    {
        this.idGeneratorFactory = idGeneratorFactory;
    }

    @Override
    public long nodes()
    {
        return idGeneratorFactory.get( IdType.NODE ).getNumberOfIdsInUse();
    }

    @Override
    public long relationships()
    {
        return idGeneratorFactory.get( IdType.RELATIONSHIP ).getNumberOfIdsInUse();
    }

    @Override
    public long properties()
    {
        return idGeneratorFactory.get( IdType.PROPERTY ).getNumberOfIdsInUse();
    }

    @Override
    public long relationshipTypes()
    {
        return idGeneratorFactory.get( IdType.RELATIONSHIP_TYPE_TOKEN ).getNumberOfIdsInUse();
    }
}
