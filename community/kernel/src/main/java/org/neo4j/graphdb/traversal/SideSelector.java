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
package org.neo4j.graphdb.traversal;

import org.neo4j.graphdb.Direction;

/**
 * Dictates which side is the current side in a bidirectional traversal to traverse
 * the next step for. For example an alternating side selector will return alternating
 * start side and end side as long as each side hasn't reached it's end.
 * 
 * @author Mattias Persson
 */
public interface SideSelector extends BranchSelector
{
    /**
     * @return the side to traverse next on, {@link Direction#OUTGOING} for start side
     * and {@link Direction#INCOMING} for end side.
     */
    Direction currentSide();
}
