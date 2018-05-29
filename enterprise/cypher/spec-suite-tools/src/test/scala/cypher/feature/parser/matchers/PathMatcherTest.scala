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

class PathMatcherTest extends ParsingTestSupport {

  test("should match a single node path") {
    new PathMatcher(new NodeMatcher(Set("L").asJava, EMPTY)) should accept(singleNodePath(node(Seq("L"))))
  }

  test("should match a path of two links") {
    val linkMatcher1 = new PathLinkMatcher(new RelationshipMatcher("T1", EMPTY),
                                           new NodeMatcher(Set("Start1").asJava, EMPTY), true)
    linkMatcher1.setRightNode(new NodeMatcher(Set("End1").asJava, EMPTY))
    val linkMatcher2 = new PathLinkMatcher(new RelationshipMatcher("T2", EMPTY),
                                           new NodeMatcher(Set("End2").asJava, EMPTY), false)
    linkMatcher2.setRightNode(new NodeMatcher(Set("Start2").asJava, EMPTY))
    val pathMatcher = new PathMatcher(List(linkMatcher1, linkMatcher2).asJava)

    pathMatcher should accept(path(pathLink(node(Seq("Start1")), relationship("T1"), node(Seq("End1"))),
                                   pathLink(node(Seq("Start2")), relationship("T2"), node(Seq("End2")))))
  }

  test("should not accept a longer path") {
    val matcher: PathMatcher = new PathMatcher(new NodeMatcher(Set("L").asJava, EMPTY))
    val expected = path(pathLink(node(Seq("Start1")), relationship("T1"), node(Seq("End1"))))

    matcher shouldNot accept(expected)
  }

  test("should not accept a shorter path") {
    val linkMatcher = new PathLinkMatcher(new RelationshipMatcher("T1", EMPTY),
                                          new NodeMatcher(Set("Start1").asJava, EMPTY), true)
    linkMatcher.setRightNode(new NodeMatcher(Set("End1").asJava, EMPTY))
    val pathMatcher = new PathMatcher(List(linkMatcher).asJava)

    pathMatcher shouldNot accept(singleNodePath(node(Seq("L"))))
  }

  test("should not accept a shorter path 2") {
    val linkMatcher1 = new PathLinkMatcher(new RelationshipMatcher("T1", EMPTY),
                                           new NodeMatcher(Set("Start1").asJava, EMPTY), true)
    linkMatcher1.setRightNode(new NodeMatcher(Set("End1").asJava, EMPTY))
    val linkMatcher2 = new PathLinkMatcher(new RelationshipMatcher("T2", EMPTY),
                                           new NodeMatcher(Set("End2").asJava, EMPTY), false)
    linkMatcher2.setRightNode(new NodeMatcher(Set("Start2").asJava, EMPTY))
    val pathMatcher = new PathMatcher(List(linkMatcher1, linkMatcher2).asJava)

    val expected = path(pathLink(node(Seq("Start1")), relationship("T1"), node(Seq("End1"))))

    pathMatcher shouldNot accept(expected)
  }

  test("should not match a path with wrong link") {
    val linkMatcher1 = new PathLinkMatcher(new RelationshipMatcher("T1", EMPTY),
                                           new NodeMatcher(Set.empty[String].asJava, EMPTY), true)
    linkMatcher1.setRightNode(new NodeMatcher(Set.empty[String].asJava, EMPTY))
    val pathMatcher = new PathMatcher(List(linkMatcher1).asJava)

    pathMatcher shouldNot accept(path(pathLink(node(), relationship("T2"), node())))
  }

  test("should not match a non-path") {
    new PathMatcher(new NodeMatcher(Set("L").asJava, EMPTY)) shouldNot accept("a string")
  }

}
