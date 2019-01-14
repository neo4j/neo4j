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
package org.neo4j.storageengine.api.schema;

import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexLimitation;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexValueCapability;
import org.neo4j.values.storable.ValueCategory;

/**
 * A committed index with specified capabilities.
 */
public class CapableIndexDescriptor extends StoreIndexDescriptor
{
    private final IndexCapability indexCapability;

    public CapableIndexDescriptor( StoreIndexDescriptor indexDescriptor, IndexCapability indexCapability )
    {
        super( indexDescriptor );
        this.indexCapability = indexCapability;
    }

    @Override
    public IndexOrder[] orderCapability( ValueCategory... valueCategories )
    {
        return indexCapability.orderCapability( valueCategories );
    }

    @Override
    public IndexValueCapability valueCapability( ValueCategory... valueCategories )
    {
        return indexCapability.valueCapability( valueCategories );
    }

    @Override
    public IndexLimitation[] limitations()
    {
        return indexCapability.limitations();
    }

    @Override
    public boolean isFulltextIndex()
    {
        return indexCapability.isFulltextIndex();
    }

    @Override
    public boolean isEventuallyConsistent()
    {
        return indexCapability.isEventuallyConsistent();
    }
}
