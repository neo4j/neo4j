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
package org.neo4j.graphalgo.impl.centrality;

import org.neo4j.graphalgo.CostAccumulator;

/**
 * To make calculations as general as possible, this follows a similar idea to
 * {@link CostAccumulator}. The core use of this is the closeness centrality's
 * need to produce an "average" value.
 * @author work
 * @param <CostType>
 */
public interface CostDivider<CostType>
{
    /**
     * @return c / d
     */
    CostType divideCost( CostType c, Double d );

    /**
     * @return d / c
     */
    CostType divideByCost( Double d, CostType c );
}
