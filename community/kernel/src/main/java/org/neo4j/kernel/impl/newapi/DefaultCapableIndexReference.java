/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexValueCapability;
import org.neo4j.values.storable.ValueGroup;

class DefaultCapableIndexReference implements org.neo4j.internal.kernel.api.CapableIndexReference
{
    private final int label;
    private final int[] properties;
    private final boolean unique;
    private final IndexCapability capability;

    DefaultCapableIndexReference( boolean unique, IndexCapability indexCapability, int label, int... properties )
    {
        this.unique = unique;
        this.capability = indexCapability;
        this.label = label;
        this.properties = properties;
    }

    @Override
    public boolean isUnique()
    {
        return unique;
    }

    @Override
    public int label()
    {
        return label;
    }

    @Override
    public int[] properties()
    {
        return properties;
    }

    @Override
    public IndexOrder[] orderCapability( ValueGroup... valueGroups )
    {
        return capability.orderCapability( valueGroups );
    }

    @Override
    public IndexValueCapability valueCapability( ValueGroup... valueGroups )
    {
        return capability.valueCapability( valueGroups );
    }
}
