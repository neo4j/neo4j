/*
 * Copyright 2008 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.shortestpath;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphalgo.shortestpath.FindPath;
import org.neo4j.graphalgo.shortestpath.Util;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;

/**
 * This is a modiciation of {@link FindSinglePath} which finds all paths, not just one
 * 
 * @see FindPath
 * @author Patrik Larsson
 */
public class FindAllShortestPaths {
	private HashMap<Node, List<Relationship>> predecessors1;
	private HashMap<Node, List<Relationship>> predecessors2;
	private final int maxDepth;
	private final Node startNode;
	private final Node endNode;
	private final Object[] relTypesAndDirections;
	private boolean doneCalculation;
	final List<Node> matchNodes = new LinkedList<Node>();

	/**
	 * Resets the result data to force the computation to be run again when some
	 * result is asked for.
	 */
	public void reset() {
		predecessors1 = new HashMap<Node, List<Relationship>>();
		predecessors2 = new HashMap<Node, List<Relationship>>();
		doneCalculation = false;
	}

	/**
	 * Makes the main calculation. If some limit is set, the shortest path(s)
	 * that could be found within those limits will be calculated.
	 * 
	 * @return True if a path was found.
	 */
	public boolean calculate() {
		// Do this first as a general error check since this is supposed to be
		// called whenever a result is asked for.
		if (startNode == null || endNode == null) {
			throw new RuntimeException("Start or end node undefined.");
		}
		// Don't do it more than once
		if (doneCalculation) {
			return true;
		}
		doneCalculation = true;
		// Special case when path length is zero
		if (startNode.equals(endNode)) {
			matchNodes.add(startNode);
			return true;
		}
		PathStopEval stopEval1 = new PathStopEval(maxDepth / 2);
		PathStopEval stopEval2 = new PathStopEval(maxDepth / 2 + maxDepth % 2);
		PathReturnEval returnEval1 = new PathReturnEval(predecessors1,
				predecessors2, matchNodes);
		PathReturnEval returnEval2 = new PathReturnEval(predecessors2,
				predecessors1, matchNodes);
		Traverser trav1 = startNode.traverse(Order.BREADTH_FIRST, stopEval1,
				returnEval1, relTypesAndDirections); // relationshipTypes,
														// directions );
		Traverser trav2 = endNode.traverse(Order.BREADTH_FIRST, stopEval2,
				returnEval2, relTypesAndDirections); // relationshipTypes,
														// directions );
		Iterator<Node> itr1 = trav1.iterator();
		Iterator<Node> itr2 = trav2.iterator();
		while (itr1.hasNext() || itr2.hasNext()) {
			if (itr1.hasNext()) {
				itr1.next();
			}
			// if ( returnEval1.getMatchNode() != null )
			// {
			// matchNode = returnEval1.getMatchNode();
			// return true;
			// }
			if (itr2.hasNext()) {
				itr2.next();
			}
			// if ( returnEval2.getMatchNode() != null )
			// {
			// matchNode = returnEval2.getMatchNode();
			// return true;
			// }
		}
		return false;
	}

	private static class PathStopEval implements StopEvaluator {
		private int maximumDepth;

		public PathStopEval(int maximumDepth) {
			super();
			this.maximumDepth = maximumDepth;
		}

		public boolean isStopNode(TraversalPosition currentPos) {
			return currentPos.depth() >= maximumDepth;
		}
	}

	private static class PathReturnEval implements ReturnableEvaluator {
		final Map<Node, List<Relationship>> myNodes;
		final Map<Node, List<Relationship>> otherNodes;
		private final List<Node> matchNodes;

		public PathReturnEval(final Map<Node, List<Relationship>> myNodes,
				final Map<Node, List<Relationship>> otherNodes,
				final List<Node> matchNodes) {
			super();
			this.myNodes = myNodes;
			this.otherNodes = otherNodes;
			this.matchNodes = matchNodes;
		}

		public boolean isReturnableNode(TraversalPosition currentPos) {
			Node currentNode = currentPos.currentNode();
			Relationship relationshipToCurrent = currentPos
					.lastRelationshipTraversed();
			if (relationshipToCurrent != null) {
				LinkedList<Relationship> predList = new LinkedList<Relationship>();
				predList.add(relationshipToCurrent);
				myNodes.put(currentNode, predList);
			} else {
				myNodes.put(currentNode, null);
			}
			if (otherNodes.containsKey(currentNode)) {
				// match
				if (!matchNodes.contains(currentNode)) {
					matchNodes.add(currentNode);
				}
			}
			return true;
		}

	}

	/**
	 * @param startNode
	 *            The node in which the path should start.
	 * @param endNode
	 *            The node in which the path should end.
	 * @param relationshipType
	 *            The type of relationship to follow.
	 * @param maxDepth
	 *            A maximum search length.
	 */
	public FindAllShortestPaths(Node startNode, Node endNode,
			RelationshipType relationshipType, int maxDepth) {
		super();
		reset();
		this.startNode = startNode;
		this.endNode = endNode;
		this.relTypesAndDirections = new Object[2];
		this.relTypesAndDirections[0] = relationshipType;
		this.relTypesAndDirections[1] = Direction.BOTH;
		this.maxDepth = maxDepth;
	}

	public FindAllShortestPaths(Node startNode, Node endNode,
			RelationshipType[] relationshipTypes, Direction[] directions,
			int maxDepth) {
		super();
		reset();
		this.startNode = startNode;
		this.endNode = endNode;
		relTypesAndDirections = new Object[relationshipTypes.length * 2];
		for (int i = 0; i < relationshipTypes.length; i++) {
			relTypesAndDirections[i * 2] = relationshipTypes[i];
			relTypesAndDirections[i * 2 + 1] = directions[i];
		}
		this.maxDepth = maxDepth;
	}

	/**
	 * @return One of the shortest paths found or null.
	 */
	public List<List<PropertyContainer>> getPaths() {
		calculate();
		if (matchNodes == null) {
			return null;
		}
		List<List<PropertyContainer>> paths = new LinkedList<List<PropertyContainer>>();
		for (Node matchNode : matchNodes) {
			LinkedList<PropertyContainer> path = new LinkedList<PropertyContainer>();
			path.addAll(Util.constructSinglePathToNode(matchNode,
					predecessors1, true, false));
			path.addAll(Util.constructSinglePathToNode(matchNode,
					predecessors2, false, true));
			paths.add(path);

		}
		return paths;
	}

	/**
	 * @return .
	 */
	public List<List<Node>> getPathsAsNodes() {
		calculate();
		if (matchNodes == null) {
			return null;
		}
		LinkedList<List<Node>> paths = new LinkedList<List<Node>>();
		for (Node matchNode : matchNodes) {
			LinkedList<Node> pathNodes = new LinkedList<Node>();
			pathNodes.addAll(Util.constructSinglePathToNodeAsNodes(matchNode,
					predecessors1, true, false));
			pathNodes.addAll(Util.constructSinglePathToNodeAsNodes(matchNode,
					predecessors2, false, true));
			paths.add(pathNodes);

		}
		return paths;
	}

	/**
	 * @return One of the shortest paths found or null.
	 */
	public List<List<Relationship>> getPathAsRelationships() {
		calculate();
		if (matchNodes == null) {
			return null;
		}
		List<List<Relationship>> paths = new LinkedList<List<Relationship>>();
		for (Node matchNode : matchNodes) {
			List<Relationship> path = new LinkedList<Relationship>();
			path.addAll(Util.constructSinglePathToNodeAsRelationships(
					matchNode, predecessors1, false));
			path.addAll(Util.constructSinglePathToNodeAsRelationships(
					matchNode, predecessors2, true));
			paths.add(path);
		}
		return paths;
	}
}
