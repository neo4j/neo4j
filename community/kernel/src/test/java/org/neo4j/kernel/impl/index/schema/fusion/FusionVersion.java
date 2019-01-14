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
package org.neo4j.kernel.impl.index.schema.fusion;

import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.LUCENE;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.NUMBER;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.SPATIAL;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.STRING;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.TEMPORAL;

enum FusionVersion
{
    v00
            {
                @Override
                IndexSlot[] aliveSlots()
                {
                    return new IndexSlot[]{LUCENE, SPATIAL, TEMPORAL};
                }

                @Override
                SlotSelector slotSelector()
                {
                    return new FusionSlotSelector00();
                }
            },
    v10
            {
                @Override
                IndexSlot[] aliveSlots()
                {
                    return new IndexSlot[]{NUMBER, LUCENE, SPATIAL, TEMPORAL};
                }

                @Override
                SlotSelector slotSelector()
                {
                    return new FusionSlotSelector10();
                }
            },
    v20
            {
                @Override
                IndexSlot[] aliveSlots()
                {
                    return new IndexSlot[]{STRING, NUMBER, SPATIAL, TEMPORAL, LUCENE};
                }

                @Override
                SlotSelector slotSelector()
                {
                    return new FusionSlotSelector20();
                }
            };

    abstract IndexSlot[] aliveSlots();

    abstract SlotSelector slotSelector();
}
