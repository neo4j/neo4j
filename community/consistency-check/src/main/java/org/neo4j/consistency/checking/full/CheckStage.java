/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.consistency.checking.full;

import static org.neo4j.consistency.checking.cache.CacheSlots.ID_SLOT_SIZE;
import static org.neo4j.consistency.checking.cache.CacheSlots.LABELS_SLOT_SIZE;

/**
 * The different stages a consistency check goes through. A stage typically focuses one one store.
 */
public enum CheckStage implements Stage
{
    Stage1_NS_PropsLabels( false, true, "NodeStore pass - check its properties, check labels and cache them, skip relationships", 1, LABELS_SLOT_SIZE ),
    Stage2_RS_Labels( false, true, "ReltionshipStore pass - check label counts using cached labels, check properties, skip nodes and relationships", 1, LABELS_SLOT_SIZE ),
    Stage3_NS_NextRel( false, true, "NodeStore pass - just cache nextRel and inUse", 1, 1, ID_SLOT_SIZE ),
    Stage4_RS_NextRel( true, true, "RelationshipStore pass - check nodes inUse, FirstInFirst, FirstInSecond using cached info", 1, 1, ID_SLOT_SIZE ),
    Stage5_Check_NextRel( false, true, "NodeRelationship cache pass - check nextRel", 1, 1, ID_SLOT_SIZE ),
    Stage6_RS_Forward( true, true, "RelationshipStore pass - forward scan of source chain using the cache", 1, 1, ID_SLOT_SIZE, ID_SLOT_SIZE ),
    Stage7_RS_Backward( true, false, "RelationshipStore pass - reverse scan of source chain using the cache", 1, 1, ID_SLOT_SIZE, ID_SLOT_SIZE ),
    Stage8_PS_Props( true, true, "PropertyStore and Node to Index check pass" ),
    Stage9_NS_LabelCounts( true, true, "NodeStore pass - Label counts" ),
    Stage10_NS_PropertyRelocator( true, true, "Property store relocation" );

    private final boolean parallel;
    private final boolean forward;
    private final String purpose;
    private final int[] cacheSlotSizes;

    private CheckStage( boolean parallel, boolean forward, String purpose, int... cacheFields )
    {
        this.parallel = parallel;
        this.forward = forward;
        this.purpose = purpose;
        this.cacheSlotSizes = cacheFields;
    }

    @Override
    public boolean isParallel()
    {
        return parallel;
    }

    @Override
    public boolean isForward()
    {
        return forward;
    }

    @Override
    public String getPurpose()
    {
        return purpose;
    }

    @Override
    public int[] getCacheSlotSizes()
    {
        return cacheSlotSizes;
    }
}
