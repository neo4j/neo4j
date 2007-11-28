package org.neo4j.util.matching;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;

public class PatternUtil
{
	public static void printGraph( PatternNode startNode, PrintStream out )
	{
		printGraph( startNode, "", new HashSet<PatternNode>(), out );
	}
	
	private static void printGraph( PatternNode startNode, String prefix,
		Set<PatternNode> visited, PrintStream out )
	{
		visited.add( startNode );
		out.println( prefix + startNode + ": " );
		for ( PatternRelationship relationship : startNode.getRelationships() )
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
