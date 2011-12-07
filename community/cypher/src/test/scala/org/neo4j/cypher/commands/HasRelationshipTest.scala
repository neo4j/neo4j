package org.neo4j.cypher.commands

import org.neo4j.cypher.GraphDatabaseTestBase
import org.scalatest.Assertions
import org.junit.{Before, Test}
import org.neo4j.graphdb.{Node, Direction}
import org.junit.Assert._


class HasRelationshipTest extends GraphDatabaseTestBase with Assertions {
  var a: Node = null
  var b: Node = null
  val aValue = EntityValue("a")
  val bValue = EntityValue("b")

  @Before
  def init() {
    a = createNode()
    b = createNode()
  }

  def createPredicate(dir: Direction, relType: Option[String]): HasRelationship = HasRelationship(EntityValue("a"), EntityValue("b"), dir, relType)

  @Test def givenTwoRelatedNodesThenReturnsTrue() {
    relate(a, b)

    val predicate = createPredicate(Direction.BOTH, None)

    assertTrue("Expected the predicate to return true, but it didn't", predicate.isMatch(Map("a" -> a, "b" -> b)))
  }

  @Test def checksTheRelationshipType() {
    relate(a, b, "KNOWS")

    val predicate = createPredicate(Direction.BOTH, Some("FEELS"))

    assertFalse("Expected the predicate to return true, but it didn't", predicate.isMatch(Map("a" -> a, "b" -> b)))
  }
}