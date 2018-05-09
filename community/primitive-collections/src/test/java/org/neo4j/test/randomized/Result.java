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
package org.neo4j.test.randomized;

public class Result<T,F>
{
    private final T target;
    private final int index;
    private final F failure;

    public Result( T target, int index, F failure )
    {
        this.target = target;
        this.index = index;
        this.failure = failure;
    }

    public T getTarget()
    {
        return target;
    }

    public int getIndex()
    {
        return index;
    }

    public F getFailure()
    {
        return failure;
    }

    public boolean isFailure()
    {
        return index != -1;
    }

    @Override
    public String toString()
    {
        return isFailure() ? "Failure[" + index + ", " + failure + "]" : "Success";
    }
}
