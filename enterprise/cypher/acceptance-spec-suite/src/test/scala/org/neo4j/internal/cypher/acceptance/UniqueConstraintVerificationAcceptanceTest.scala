package org.neo4j.internal.cypher.acceptance
import org.neo4j.cypher.{CypherExecutionException, ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.graphdb.Label
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException

import scala.collection.JavaConverters._

class UniqueConstraintVerificationAcceptanceTest
  extends ExecutionEngineFunSuite with QueryStatisticsTestSupport {

  test("should add constraint with no existing data") {
    //GIVEN

    //WHEN
    execute("create constraint on (node:Label) assert node.propertyKey is unique")

    //THEN
    graph.inTx {
      val constraints = graph.schema().getConstraints(Label.label("Label")).asScala
      constraints should have size 1
      constraints.head.getPropertyKeys.asScala.toList should equal(List("propertyKey"))
    }
  }

  test("should add constraint when existing data is unique") {
    // GIVEN
    execute("create (a:Person{name:\"Alistair\"}), (b:Person{name:\"Stefan\"})")

    // WHEN
    execute("create constraint on (n:Person) assert n.name is unique")

    // THEN
    graph.inTx {
      val constraints = graph.schema().getConstraints(Label.label("Person")).asScala
      constraints should have size 1
      constraints.head.getPropertyKeys.asScala.toList should equal(List("name"))
    }
  }

  test("should add constraint using recreated unique data") {
    // GIVEN
    execute("create (a:Person{name:\"Alistair\"}), (b:Person{name:\"Stefan\"})")
    execute("match (n:Person) delete n")
    execute("create (a:Person{name:\"Alistair\"}), (b:Person{name:\"Stefan\"})")

    // WHEN
    execute("create constraint on (n:Person) assert n.name is unique")

    // THEN
    graph.inTx {
      val constraints = graph.schema().getConstraints(Label.label("Person")).asScala
      constraints should have size 1
      constraints.head.getPropertyKeys.asScala.toList should equal(List("name"))
    }
  }

  test("should drop constraint") {
    //GIVEN
    execute("create constraint on (node:Label) assert node.propertyKey is unique")

    //WHEN
    execute("drop constraint on (node:Label) assert node.propertyKey is unique")

    //THEN
    graph.inTx {
      val constraints = graph.schema().getConstraints(Label.label("Label")).asScala
      constraints shouldBe empty
    }
  }

  test("should fail to add constraint when existing data conflicts") {
    // GIVEN
    execute("create (a:Person{id:1}), (b:Person{id:1})")

    // WHEN
    try
    {
      execute("create constraint on (n:Person) assert n.id is unique")

      fail("expected exception")
    }
    // THEN
    catch
      {
        case ex: CypherExecutionException =>
          assert(ex.getCause.isInstanceOf[CreateConstraintFailureException])
      }

    graph.inTx {
      val constraints = graph.schema().getConstraints(Label.label("Person")).asScala
      constraints shouldBe empty
    }
  }
}
