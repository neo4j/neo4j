package org.neo4j.util.matching;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
	
	public Iterable<PatternMatch> match( PatternNode start,
		Node startNode, Collection<PatternNode> optional )
	{
		if ( optional == null || optional.size() < 1 )
		{
			return new PatternFinder( start, startNode );
		}
		else
		{
			return new PatternFinder( start, startNode, false, optional );
		}
	}
	
	public Iterable<PatternMatch> match( PatternNode start,
		Node startNode, PatternNode... optional )
	{
		return match( start, startNode, Arrays.asList( optional ) );
	}
	
	private static class OptionalPatternFinder
	{
		private List<PatternFinder> optionalFinders;
		private List<PatternMatch> currentMatches;
		private Collection<PatternNode> optionalNodes;
		private PatternMatch baseMatch;
		private int position = -1;
		
		OptionalPatternFinder( PatternMatch baseMatch,
			Collection<PatternNode> optionalNodes )
		{
			this.baseMatch = baseMatch;
			this.optionalNodes = optionalNodes;
			initialize();
		}
		
		private boolean first = true;
		PatternMatch findNextOptionalPatterns()
		{
			if ( position < 0 )
			{
				return null;
			}

			if ( first && anyMatchFound() )
			{
				first = false;
				return PatternMatch.merge( currentMatches );
			}

			boolean found = false;
			for ( ; position >= 0; position-- )
			{
				if ( optionalFinders.get( position ).hasNext() )
				{
					currentMatches.set(
						position, optionalFinders.get( position ).next() );
					if ( position < currentMatches.size() - 1 )
					{
						position++;
						reset( position );
					}
					found = true;
					break;
				}
			}
			
			if ( !found )
			{
				return null;
			}
			
			return PatternMatch.merge( currentMatches );
		}
		
		boolean anyMatchFound()
		{
			return !currentMatches.isEmpty();
		}
		
		private void initialize()
		{
			optionalFinders = new ArrayList<PatternFinder>();
			currentMatches = new ArrayList<PatternMatch>();
			
			for ( PatternNode node : optionalNodes )
			{
				PatternFinder finder = new PatternFinder(
					node, this.getNodeFor( node ), true );
				if ( finder.hasNext() )
				{
					optionalFinders.add( finder );
					currentMatches.add( finder.next() );
					position++;
				}
			}
		}
		
		private Node getNodeFor( PatternNode node )
		{
			for ( PatternElement element : baseMatch.getElements() )
			{
				if ( node.getLabel().equals(
					element.getPatternNode().getLabel() ) )
				{
					return element.getNode();
				}
			}
			throw new RuntimeException(
				"Optional graph isn't connected to the main graph." );
		}

		private void reset( int fromIndex )
		{
			for ( int i = fromIndex; i < optionalFinders.size(); i++ )
			{
				PatternFinder finder = optionalFinders.get( i );
				PatternFinder newFinder = new PatternFinder(
					finder.getStartPatternNode(), finder.getStartNode(),
					true );
				optionalFinders.set( i, newFinder );
				// Only patterns with matches were added in the first place,
				// so newFinder must have at least one match.
				currentMatches.set( i, newFinder.next() );
				position = i;
			}
		}
	}
	
	private static class PatternFinder implements Iterable<PatternMatch>,
		Iterator<PatternMatch>
	{
		private Set<Relationship> visitedRels = 
			new HashSet<Relationship>();
		private PatternPosition currentPosition;
		private OptionalPatternFinder optionalFinder;
		private PatternNode startPatternNode;
		private Node startNode;
		private Collection<PatternNode> optionalNodes;
		private boolean optional;
		
		PatternFinder( PatternNode start, Node startNode )
		{
			this( start, startNode, false );
		}
		
		PatternFinder( PatternNode start, Node startNode, boolean optional )
		{
			this.startPatternNode = start;
			this.startNode = startNode;
			currentPosition =
				new PatternPosition( startNode, start, optional );
			this.optional = optional;
		}
		
		PatternFinder( PatternNode start, Node startNode, boolean optional,
			PatternNode... optionalNodes )
		{
			this( start, startNode, optional, Arrays.asList( optionalNodes ) );
		}
		
		PatternFinder( PatternNode start, Node startNode, boolean optional,
			Collection<PatternNode> optionalNodes )
		{
			this( start, startNode, optional );
			this.optionalNodes = optionalNodes;
		}
		
		PatternNode getStartPatternNode()
		{
			return startPatternNode;
		}
		
		Node getStartNode()
		{
			return startNode;
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
						filteredElements );
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
					otherPosition, optional ), true ) )
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
						otherPosition, optional ), true ) )
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
			Object[] patternValues =
				patternNode.getPropertyValue( propertyName );
			Object neoValue = neoNode.getProperty( propertyName );
			Collection<Object> neoValues =
				NeoArrayPropertyUtil.neoValueToCollection( neoValue );
			neoValues.retainAll( Arrays.asList( patternValues ) );
			return !neoValues.isEmpty();
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
		private PatternMatch optionalMatch = null;
		
		public boolean hasNext()
        {
			if ( match == null )
			{
				match = findNextMatch();
				optionalFinder = null;
			}
			else if ( optionalNodes != null )
			{
				if ( optionalFinder == null )
				{
					optionalFinder = new OptionalPatternFinder(
						match, optionalNodes );
				}
				if ( optionalMatch == null )
				{
					optionalMatch =
						optionalFinder.findNextOptionalPatterns();
				}
				if ( optionalMatch == null && optionalFinder.anyMatchFound() )
				{
					match = null;
					return hasNext();
				}
			}
			return match != null;
        }

		public PatternMatch next()
        {
			if ( match == null )
			{
				match = findNextMatch();
				optionalFinder = null;
			}
			
			PatternMatch matchToReturn = match;
			PatternMatch optionalMatchToReturn = null;
			if ( match != null && optionalNodes != null )
			{
				if ( optionalFinder == null )
				{
					optionalFinder = new OptionalPatternFinder(
						match, optionalNodes );
				}
				if ( optionalMatch == null )
				{
					optionalMatch = optionalFinder.findNextOptionalPatterns();
				}
				optionalMatchToReturn = optionalMatch;
				optionalMatch = null;
				if ( optionalMatchToReturn == null )
				{
					match = null;
					if ( optionalFinder.anyMatchFound() )
					{
						return next();
					}
				}
			}
			else
			{
				match = null;
			}
			if ( matchToReturn == null )
			{
				throw new NoSuchElementException();
			}
			return optionalMatchToReturn != null ?
				PatternMatch.merge( matchToReturn, optionalMatchToReturn ) :
					matchToReturn;
        }

		public void remove()
        {
			throw new UnsupportedOperationException();
        }
	}
}
