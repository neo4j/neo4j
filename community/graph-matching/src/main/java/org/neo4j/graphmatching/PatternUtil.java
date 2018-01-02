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
package org.neo4j.graphmatching;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;

/**
 * Utility methods for working with a pattern graph.
 */
@Deprecated
public class PatternUtil
{
    /**
     * Print a pattern graph rooted at a particular {@link PatternNode} to the
     * specified {@link PrintStream}.
     *
     * @param startNode the root of the pattern graph.
     * @param out the stream to print to.
     */
	public static void printGraph( PatternNode startNode, PrintStream out )
	{
		printGraph( startNode, "", new HashSet<PatternNode>(), out );
	}

	private static void printGraph( PatternNode startNode, String prefix,
		Set<PatternNode> visited, PrintStream out )
	{
		visited.add( startNode );
		out.println( prefix + startNode + ": " );
		for ( PatternRelationship relationship :
			startNode.getAllRelationships() )
		{
			out.print( prefix + "\t" + relationship );
			out.println( ": " +
				relationship.getOtherNode( startNode ) );
			if ( !visited.contains( relationship.getOtherNode( startNode ) ) )
			{
				printGraph( relationship.getOtherNode( startNode ),
					prefix + "\t\t", visited, out );
			}
		}
	}
}
