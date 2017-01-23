/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package cypher.feature.parser.matchers

import cypher.feature.parser.ParsingTestSupport
import cypher.feature.parser.matchers.MapMatcher.EMPTY

class PathLinkMatcherTest extends ParsingTestSupport {

  test("should match an outgoing pathlink") {
    val matcher = new PathLinkMatcher(new RelationshipMatcher("T", EMPTY), new NodeMatcher(Set("L").asJava, EMPTY), true)
    matcher.setRightNode(new NodeMatcher(Set.empty[String].asJava, EMPTY))

    matcher should accept(pathLink(node(Seq("L")), relationship("T"), node()))
  }

  test("should match an incoming pathlink") {
    val matcher = new PathLinkMatcher(new RelationshipMatcher("T", EMPTY), new NodeMatcher(Set("L").asJava, EMPTY), false)
    matcher.setRightNode(new NodeMatcher(Set.empty[String].asJava, EMPTY))

    matcher should accept(pathLink(node(), relationship("T"), node(Seq("L"))))
  }

  test("should not match a reversed pathlink") {
    val matcher = new PathLinkMatcher(new RelationshipMatcher("T", EMPTY), new NodeMatcher(Set("L").asJava, EMPTY), false)
    matcher.setRightNode(new NodeMatcher(Set.empty[String].asJava, EMPTY))

    matcher shouldNot accept(pathLink(node(Seq("L")), relationship("T"), node()))
  }

  test("should not match a different relationship") {
    val matcher = new PathLinkMatcher(new RelationshipMatcher("T1", EMPTY), new NodeMatcher(Set("L").asJava, EMPTY), true)
    matcher.setRightNode(new NodeMatcher(Set.empty[String].asJava, EMPTY))

    matcher shouldNot accept(pathLink(node(Seq("L")), relationship("T2"), node()))
  }

  test("should not match a different startNode") {
    val matcher = new PathLinkMatcher(new RelationshipMatcher("T", EMPTY), new NodeMatcher(Set("L1").asJava, EMPTY), true)
    matcher.setRightNode(new NodeMatcher(Set.empty[String].asJava, EMPTY))

    matcher shouldNot accept(pathLink(node(Seq("L2")), relationship("T"), node()))
  }

  test("should not match a non-relationship") {
    val matcher = new PathLinkMatcher(new RelationshipMatcher("T", EMPTY), new NodeMatcher(Set("L1").asJava, EMPTY), true)
    matcher.setRightNode(new NodeMatcher(Set.empty[String].asJava, EMPTY))

    matcher shouldNot accept("a string")
  }

}
