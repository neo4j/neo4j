package org.neo4j.util.matching;

import java.util.HashSet;
import java.util.Iterator;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Stack;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;

public class PatternMatcher
{
	private static PatternMatcher matcher = new PatternMatcher();
	
	private PatternMatcher()
	{
	}
	
	public static PatternMatcher getMatcher()
	{
		return matcher;
	}
	
	public Iterable<PatternMatch> match( PatternNode start, 
		Node startNode )
	{
		return new PatternFinder( start, startNode );
	}
	
	private static class PatternFinder implements Iterable<PatternMatch>,
		Iterator<PatternMatch>
	{
		private Set<Relationship> visitedRels = 
			new HashSet<Relationship>();
		private PatternPosition currentPosition;
		
		PatternFinder( PatternNode start, Node startNode )
		{
			currentPosition = new PatternPosition( startNode, start );
		}
		
		private static class CallPosition
		{
			private PatternPosition patternPosition;
			private Iterator<Relationship> relItr;
			private Relationship lastRel;
			private PatternRelationship currentPRel;
			private boolean popUncompleted;
			
			CallPosition( PatternPosition patternPosition, 
				Relationship lastRel,  
				Iterator<Relationship> relItr, 
				PatternRelationship currentPRel, boolean popUncompleted )
			{
				this.patternPosition = patternPosition;
				this.relItr = relItr;
				this.lastRel = lastRel;
				this.currentPRel = currentPRel;
				this.popUncompleted = popUncompleted;
			}

			public void setLastVisitedRelationship( Relationship rel )
            {
				this.lastRel = rel;
            }
			
			public Relationship getLastVisitedRelationship()
			{
				return lastRel;
			}

			public boolean shouldPopUncompleted()
            {
	            return popUncompleted;
            }

			public PatternPosition getPatternPosition()
            {
				return patternPosition;
            }

			public PatternRelationship getPatternRelationship()
            {
				return currentPRel;
            }

			public Iterator<Relationship> getRelationshipIterator()
            {
				return relItr;
            }
		}
		
		private Stack<CallPosition> callStack = 
			new Stack<CallPosition>();
		private Stack<PatternPosition> uncompletedPositions = 
			new Stack<PatternPosition>();
		private Stack<PatternElement> foundElements = 
			new Stack<PatternElement>();
		
		private PatternMatch findNextMatch()
		{
			if ( callStack.isEmpty() && currentPosition != null )
			{
				// try find first match
				if ( traverse( currentPosition, true ) )
				{
					// found first match, return it
					currentPosition = null;
					HashMap<PatternNode,PatternElement> filteredElements =
						new HashMap<PatternNode, PatternElement>();
					for ( PatternElement element : foundElements )
					{
						filteredElements.put( element.getPatternNode(), element );
					}
					PatternMatch patternMatch = new PatternMatch( 
						filteredElements);
					foundElements.pop();
					return patternMatch;
				}
				currentPosition = null;
			}
			else if ( !callStack.isEmpty() )
			{
				// try find other match from last found match
				boolean matchFound = false;
				do
				{
					CallPosition callStackInformation = callStack.peek();
					matchFound = traverse( callStackInformation );
				} while ( !callStack.isEmpty() && !matchFound ); 
				if ( matchFound )
				{
					// found another match, returning it
					HashMap<PatternNode,PatternElement> filteredElements =
						new HashMap<PatternNode, PatternElement>();
					for ( PatternElement element : foundElements )
					{
						filteredElements.put( element.getPatternNode(), element );
					}
					PatternMatch patternMatch = new PatternMatch( 
						filteredElements);
					foundElements.pop();
					return patternMatch;
				}
			}
			return null;
		}
		
		private boolean traverse( CallPosition callPos )
		{
			// make everything like it was before we returned previous match
			PatternPosition currentPos = callPos.getPatternPosition();
			PatternRelationship pRel = callPos.getPatternRelationship();
			pRel.mark();
			visitedRels.remove( callPos.getLastVisitedRelationship() );
			Node currentNode = currentPos.getCurrentNode();
			// String posStr = "(" + 
			// 	currentPos.getCurrentNode().getProperty( "name" ) + ")";
			//System.out.print( posStr );
			Iterator<Relationship> relItr = callPos.getRelationshipIterator();
			while ( relItr.hasNext() )
			{
				Relationship rel = relItr.next();
				if ( visitedRels.contains( rel ) )
				{
					continue;
				}
				Node otherNode = rel.getOtherNode( currentNode );
				PatternNode otherPosition = pRel.getOtherNode( 
					currentPos.getPatternNode() );
				pRel.mark();
				visitedRels.add( rel );
				if ( traverse( new PatternPosition( otherNode, 
					otherPosition ), true ) )
				{
					callPos.setLastVisitedRelationship( rel );
					return true;
				}
				visitedRels.remove( rel );
				pRel.unMark();
			}
			pRel.unMark();
			if ( callPos.shouldPopUncompleted() )
			{
				uncompletedPositions.pop();
			}
			callStack.pop();
			foundElements.pop();
			return false;
		}
		
		private boolean traverse( PatternPosition currentPos, 
			boolean pushElement )
		{
			PatternNode pNode = currentPos.getPatternNode();
			Node currentNode = currentPos.getCurrentNode();
			
			if ( !checkProperties( pNode, currentNode ) )
			{
				return false;
			}
				
			if ( pushElement )
			{
				foundElements.push( new PatternElement( 
					currentPos.getPatternNode(), 
					currentPos.getCurrentNode() ) );
			}
			// String posStr = "[" + 
			// 	currentPos.getCurrentNode().getProperty( "name" ) + "]";
			// System.out.print( posStr );
			if ( currentPos.hasNext() )
			{
				boolean popUncompleted = false;
				PatternRelationship pRel = currentPos.next();
				if ( currentPos.hasNext() )
				{
					uncompletedPositions.push( currentPos );
					popUncompleted = true;
				}
				assert !pRel.isMarked();
				Iterator<Relationship> relItr = 
					currentNode.getRelationships( pRel.getType(), 
						pRel.getDirectionFrom( 
							currentPos.getPatternNode() ) ).iterator();
				pRel.mark();
				while ( relItr.hasNext() )
				{
					Relationship rel = relItr.next();
					if ( visitedRels.contains( rel ) )
					{
						continue;
					}
					Node otherNode = rel.getOtherNode( currentNode );
					PatternNode otherPosition = pRel.getOtherNode( 
						currentPos.getPatternNode() );
					visitedRels.add( rel );
					
					CallPosition callPos = new CallPosition(
						currentPos, rel, relItr, pRel, popUncompleted );
					callStack.push( callPos );
					if ( traverse( new PatternPosition( otherNode, 
						otherPosition ), true ) )
					{
						return true;
					}
					callStack.pop();
					visitedRels.remove( rel );
				}
				pRel.unMark();
				if ( popUncompleted )
				{
					uncompletedPositions.pop();
				}
				foundElements.pop();
				return false;
			}
			boolean matchFound = true;
			if ( !uncompletedPositions.isEmpty() )
			{
				PatternPosition digPos = uncompletedPositions.pop();
				digPos.reset();
				//System.out.print( "," );
				matchFound = traverse( digPos, false );
				uncompletedPositions.push( digPos );
				return matchFound;
			}
			return true;
		}

		private boolean checkProperties(
			PatternNode patternNode, Node neoNode )
		{
			for ( String propertyName : patternNode.getPropertiesExist() )
			{
				if ( !neoNode.hasProperty( propertyName ) )
				{
					return false;
				}
			}
			
			for ( String propertyName : patternNode.getPropertiesEqual() )
			{
//				if ( !neoNode.hasProperty( propertyName ) ||
//					!patternNode.getPropertyValue( propertyName ).equals(
//					neoNode.getProperty( propertyName ) ) )
				if ( !neoPropertyHasValue( neoNode, patternNode,
					propertyName ) )
				{
					return false;
				}
			}
			
			return true;
		}
		
		private boolean neoPropertyHasValue( Node neoNode,
			PatternNode patternNode, String propertyName )
		{
			if ( !neoNode.hasProperty( propertyName ) )
			{
				return false;
			}
			Object patternValue = patternNode.getPropertyValue( propertyName );
			Object neoValue = neoNode.getProperty( propertyName );
			return NeoArrayPropertyUtil.neoValueToCollection(
				neoValue ).contains( patternValue );
		}

// old implementation that doesn't return values
//		private boolean traverse( PatternPosition currentPos, 
//			Stack<PatternPosition> uncompletedPositions, 
//			Stack<Node> foundNodes )
//		{
//			foundNodes.push( currentPos.getCurrentNode() );
//			//String posStr = "[" + currentPos.getCurrentNode().getProperty( "name" ) + "]";
//			if ( currentPos.hasNext() )
//			{
//				boolean popUncompleted = false;
//				boolean anyMatchFound = false;
//				try
//				{
//					PatternRelationship pRel = currentPos.next();
//					if ( currentPos.hasNext() )
//					{
//						uncompletedPositions.push( currentPos );
//						popUncompleted = true;
//					}
//					assert !pRel.isMarked();
//					Node currentNode = currentPos.getCurrentNode();
//					for ( Relationship rel : currentNode.getRelationships( 
//							pRel.getType(), pRel.getDirectionFrom( 
//								currentPos.getPatternNode() ) ) )
//					{
//						if ( visitedRels.contains( rel ) )
//						{
//							continue;
//						}
//						Node otherNode = rel.getOtherNode( currentNode );
//						PatternNode otherPosition = pRel.getOtherNode( 
//							currentPos.getPatternNode() );
//						pRel.mark();
//						visitedRels.add( rel );
//						
//						if ( traverse( new PatternPosition( otherNode, 
//							otherPosition ), uncompletedPositions, 
//							foundNodes ) )
//						{
//							anyMatchFound = true;
//						}
//						visitedRels.remove( rel );
//						pRel.unMark();
//					}
//					return anyMatchFound;
//				}
//				finally
//				{
//					if ( popUncompleted )
//					{
//						uncompletedPositions.pop();
//					}
//					foundNodes.pop();
//				}
//			}
//			boolean matchFound = true;
//			if ( !uncompletedPositions.isEmpty() )
//			{
//				PatternPosition digPos = uncompletedPositions.pop();
//				digPos.reset();
//				matchFound = traverse( digPos, uncompletedPositions, 
//					foundNodes );
//				uncompletedPositions.push( digPos );
//			}
//			else
//			{
//				for ( Node node : foundNodes )
//				{
//					System.out.print( node.getProperty( "name" ) + " " );
//				}
//				System.out.println( " tt " );
//			}
//			foundNodes.pop();
//			return matchFound;
//		}
		
		public Iterator<PatternMatch> iterator()
        {
			return this;
        }

		private PatternMatch match = null;
		
		public boolean hasNext()
        {
			if ( match == null )
			{
				match = findNextMatch();
			}
			return match != null;
        }

		public PatternMatch next()
        {
			if ( match == null )
			{
				match = findNextMatch();
			}
			PatternMatch matchToReturn = match;
			match = null;
			if ( matchToReturn == null )
			{
				throw new NoSuchElementException();
			}
			return matchToReturn;
        }

		public void remove()
        {
			throw new UnsupportedOperationException();
        }
	}
}
