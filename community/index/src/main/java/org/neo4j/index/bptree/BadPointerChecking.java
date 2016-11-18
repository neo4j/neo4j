/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.bptree;

public class BadPointerChecking
{
    /**
     * Arriving here is actually quite bad is it signals some sort of inconsistency/corruption in the tree.
     * TODO Can we do something here, repair perhaps?
     */
    public static void communicateBadPointer( long result )
    {
        // TODO: The NO_NODE_FLAG being -1 generally conflicts with how GSPP results are built up,
        // most notably -1 sets all bits to 1 and so any additional flags are overwritten.
        // As a work-around we can for the time being check -1 explicitly before checking flags.

        // TODO: include information on current page and perhaps path here
        if ( result == TreeNode.NO_NODE_FLAG )
        {
            throw new IllegalStateException( "Generally uninitialized GSPP" );
        }

        if ( !GenSafePointerPair.isSuccess( result ) )
        {
            throw new IllegalStateException( GenSafePointerPair.failureDescription( result ) );
        }
    }
}
