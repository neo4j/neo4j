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
package org.neo4j.kernel.impl.index.schema.fusion;

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.LUCENE;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.NUMBER;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.SPATIAL;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.STRING;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.TEMPORAL;

enum FusionVersion
{
    v00
            {
                @Override
                int[] aliveSlots()
                {
                    return new int[]{LUCENE, SPATIAL, TEMPORAL};
                }

                @Override
                FusionIndexProvider.Selector selector()
                {
                    return new FusionSelector00();
                }
            },
    v10
            {
                @Override
                int[] aliveSlots()
                {
                    return new int[]{NUMBER, LUCENE, SPATIAL, TEMPORAL};
                }

                @Override
                FusionIndexProvider.Selector selector()
                {
                    return new FusionSelector10();
                }
            },
    v20
            {
                @Override
                int[] aliveSlots()
                {
                    return new int[]{STRING, NUMBER, SPATIAL, TEMPORAL, LUCENE};
                }

                @Override
                FusionIndexProvider.Selector selector()
                {
                    return new FusionSelector20();
                }
            };

    abstract int[] aliveSlots();

    abstract FusionIndexProvider.Selector selector();
}
