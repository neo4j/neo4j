/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.util.function.Function;

import org.neo4j.internal.schema.IndexBehaviour;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexOrderCapability;
import org.neo4j.internal.schema.IndexValueCapability;
import org.neo4j.values.storable.ValueCategory;

public class FusionIndexCapability implements IndexCapability
{
    private static final Function<ValueCategory,ValueCategory> categoryOf = Function.identity();
    private final SlotSelector slotSelector;
    private final InstanceSelector<IndexCapability> instanceSelector;
    private final IndexBehaviour[] behaviours;

    FusionIndexCapability( SlotSelector slotSelector, InstanceSelector<IndexCapability> instanceSelector )
    {
        this.slotSelector = slotSelector;
        this.instanceSelector = instanceSelector;
        this.behaviours = buildBehaviours( slotSelector );
    }

    private static IndexBehaviour[] buildBehaviours( SlotSelector slotSelector )
    {
        // If we delegate single property text queries to anything else than Lucene, we have slow contains
        IndexSlot slot = slotSelector.selectSlot( new ValueCategory[]{ValueCategory.TEXT}, categoryOf );
        if ( slot != IndexSlot.LUCENE )
        {
            return new IndexBehaviour[]{IndexBehaviour.SLOW_CONTAINS};
        }
        else
        {
            return new IndexBehaviour[0];
        }
    }

    @Override
    public IndexOrderCapability orderCapability( ValueCategory... valueCategories )
    {
        IndexSlot slot = slotSelector.selectSlot( valueCategories, categoryOf );
        if ( slot == null )
        {
            return IndexOrderCapability.NONE;
        }
        return instanceSelector.select( slot ).orderCapability( valueCategories );
    }

    @Override
    public IndexValueCapability valueCapability( ValueCategory... valueCategories )
    {
        IndexSlot slot = slotSelector.selectSlot( valueCategories, categoryOf );
        if ( slot == null )
        {
            return IndexValueCapability.PARTIAL;
        }
        return instanceSelector.select( slot ).valueCapability( valueCategories );
    }

    @Override
    public IndexBehaviour[] behaviours()
    {
        return behaviours;
    }
}
