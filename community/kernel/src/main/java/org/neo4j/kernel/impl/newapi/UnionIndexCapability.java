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
package org.neo4j.kernel.impl.newapi;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexLimitation;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexValueCapability;
import org.neo4j.values.storable.ValueCategory;

/**
 * Present the union of multiple index capabilities.
 * If one of the internal capabilities has a capability, the union has the capability.
 */
public class UnionIndexCapability implements IndexCapability
{
    private final IndexCapability[] capabilities;
    private final IndexLimitation[] limitationsUnion;

    public UnionIndexCapability( IndexCapability... capabilities )
    {
        this.capabilities = capabilities;
        this.limitationsUnion = limitationsUnion( capabilities );
    }

    @Override
    public IndexOrder[] orderCapability( ValueCategory... valueCategories )
    {
        Set<IndexOrder> orderCapability = new HashSet<>();
        for ( IndexCapability capability : capabilities )
        {
            orderCapability.addAll( Arrays.asList( capability.orderCapability( valueCategories ) ) );
        }
        return orderCapability.toArray( new IndexOrder[orderCapability.size()] );
    }

    @Override
    public IndexValueCapability valueCapability( ValueCategory... valueCategories )
    {
        IndexValueCapability currentBest = IndexValueCapability.NO;
        for ( IndexCapability capability : capabilities )
        {
            IndexValueCapability next = capability.valueCapability( valueCategories );
            if ( next.compare( currentBest ) > 0 )
            {
                currentBest = next;
            }
        }
        return currentBest;
    }

    @Override
    public IndexLimitation[] limitations()
    {
        return limitationsUnion;
    }

    private IndexLimitation[] limitationsUnion( IndexCapability[] capabilities )
    {
        HashSet<IndexLimitation> union = new HashSet<>();
        for ( IndexCapability capability : capabilities )
        {
            union.addAll( Arrays.asList( capability.limitations() ) );
        }
        return union.toArray( new IndexLimitation[union.size()] );
    }
}
