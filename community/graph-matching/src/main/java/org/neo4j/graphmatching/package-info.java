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
/**
 * 
 * Facilities for finding occurrences of patterns in a Neo4j graph.
 * 
 * The main classes for defining a graph pattern is the
 * {@link org.neo4j.graphmatching.PatternNode} and the
 * {@link org.neo4j.graphmatching.PatternRelationship}. The actual matching
 * is performed by the {@link org.neo4j.graphmatching.PatternMatcher}.
 *
 * A simple example of matching a friend of a friend pattern:
 * <pre><code>
 * {@link org.neo4j.graphmatching.PatternNode}   me = {@link org.neo4j.graphmatching.PatternNode#PatternNode() new PatternNode()},
 *         myFriend = {@link org.neo4j.graphmatching.PatternNode#PatternNode() new PatternNode()},
 * friendOfMyFriend = {@link org.neo4j.graphmatching.PatternNode#PatternNode() new PatternNode()};
 *
 * me.{@link org.neo4j.graphmatching.PatternNode#createRelationshipTo(org.neo4j.graphmatching.PatternNode, org.neo4j.graphdb.RelationshipType, org.neo4j.graphdb.Direction) createRelationshipTo}( myFriend, {@link org.neo4j.graphdb.RelationshipType FoafDomain.FRIEND}, {@link org.neo4j.graphdb.Direction#BOTH Direction.BOTH} );
 * myFriend.{@link org.neo4j.graphmatching.PatternNode#createRelationshipTo(org.neo4j.graphmatching.PatternNode, org.neo4j.graphdb.RelationshipType, org.neo4j.graphdb.Direction) createRelationshipTo}( friendOfMyFriend, {@link org.neo4j.graphdb.RelationshipType FoafDomain.FRIEND}, {@link org.neo4j.graphdb.Direction#BOTH Direction.BOTH} );
 *
 * {@link org.neo4j.graphmatching.PatternMatcher} matcher = {@link org.neo4j.graphmatching.PatternMatcher#getMatcher() PatternMatcher.getMatcher()};
 * for ( {@link org.neo4j.graphmatching.PatternMatch} match : matcher.{@link org.neo4j.graphmatching.PatternMatcher#match(PatternNode, org.neo4j.graphdb.Node) match}( me, {@link org.neo4j.graphdb.Node startNode} ) )
 * {
 *     {@link org.neo4j.graphdb.Node} foaf = match.{@link org.neo4j.graphmatching.PatternMatch#getNodeFor(PatternNode) getNodeFor}( friendOfMyFriend );
 * }
 * </code></pre>
 */
@Deprecated
package org.neo4j.graphmatching;