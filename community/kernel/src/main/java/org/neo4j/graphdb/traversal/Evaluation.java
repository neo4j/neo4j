/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

/**
 * Result of {@link Evaluator#evaluate(org.neo4j.graphdb.Path)}.
 * 
 * @author Mattias Persson
 * @see Evaluator
 */
public enum Evaluation
{
    INCLUDE_AND_CONTINUE( true, true ),
    INCLUDE_AND_PRUNE( true, false ),
    EXCLUDE_AND_CONTINUE( false, true ),
    EXCLUDE_AND_PRUNE( false, false );
    
    private final boolean includes;
    private final boolean continues;

    private Evaluation( boolean includes, boolean continues )
    {
        this.includes = includes;
        this.continues = continues;
    }
    
    public boolean includes()
    {
        return this.includes;
    }
    
    public boolean continues()
    {
        return continues;
    }
    
    public static Evaluation of( boolean includes, boolean continues )
    {
        return includes?(continues?INCLUDE_AND_CONTINUE:INCLUDE_AND_PRUNE):
                        (continues?EXCLUDE_AND_CONTINUE:EXCLUDE_AND_PRUNE);
    }
    
    /**
     * Include or exclude; always continue.
     */
    public static Evaluation ofIncludes( boolean includes )
    {
        return includes?INCLUDE_AND_CONTINUE:EXCLUDE_AND_CONTINUE;
    }
    
    /**
     * Continue or prune; always include.
     */
    public static Evaluation ofContinues( boolean continues )
    {
        return continues?INCLUDE_AND_CONTINUE:INCLUDE_AND_PRUNE;
    }
}
