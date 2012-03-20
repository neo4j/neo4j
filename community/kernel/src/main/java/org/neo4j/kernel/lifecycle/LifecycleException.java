/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

package org.neo4j.kernel.lifecycle;

/**
 * This exception is thrown by LifeSupport if a lifecycle transition fails. If many exceptions occur
 * they will be chained through the cause exception mechanism.
 */
public class LifecycleException
    extends RuntimeException
{
    LifecycleStatus from;
    LifecycleStatus to;
    
    public LifecycleException( Object instance, LifecycleStatus from, LifecycleStatus to, Throwable cause )
    {
        super("Failed to transition "+instance.toString()+" from "+from.name()+" to "+to.name(), cause);
        this.from = from;
        this.to = to;
    }
}
