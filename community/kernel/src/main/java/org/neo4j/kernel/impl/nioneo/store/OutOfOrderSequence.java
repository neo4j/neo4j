/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

/**
 * The thinking behind an out-of-order sequence is that, to the outside, there's one "last number"
 * which will will never be decremented between times of looking at it. It can move in bigger strides
 * than 1 though. That is because multiple threads can tell it that a certain number is "done",
 * a number that not necessarily is the last one plus one. So if a gap is observed then the number
 * that is the logical next one, whenever that arrives, will move the externally visible number to
 * the highest gap-free number set.
 */
public interface OutOfOrderSequence
{
    void offer( long number );

    long get();
}
