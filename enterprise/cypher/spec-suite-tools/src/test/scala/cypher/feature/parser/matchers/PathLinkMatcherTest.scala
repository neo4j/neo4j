/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
