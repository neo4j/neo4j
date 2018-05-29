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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v3_2.helpers.ListSupport
import org.neo4j.cypher.{CypherExecutionException, ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory
import org.neo4j.kernel.impl.api.OperationsFacade

import scala.collection.JavaConverters._

class UniqueConstraintVerificationAcceptanceTest
  extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with ListSupport {

  test("should_add_constraint_with_no_existing_data") {
    //GIVEN

    //WHEN
    execute("create constraint on (node:Label) assert node.propertyKey is unique")

    //THEN
    graph.inTx {
      context: OperationsFacade =>
        val prop = context.propertyKeyGetForName("propertyKey")
        val label = context.labelGetForName("Label")

        val constraints = context.constraintsGetForSchema(SchemaDescriptorFactory.forLabel(label, prop)).asScala

        constraints should have size 1
    }
  }

  test("should_add_constraint_when_existing_data_is_unique") {
    // GIVEN
    execute("create (a:Person{name:\"Alistair\"}), (b:Person{name:\"Stefan\"})")

    // WHEN
    execute("create constraint on (n:Person) assert n.name is unique")

    // THEN
    graph.inTx {
      context: OperationsFacade =>
        val prop = context.propertyKeyGetForName("name")
        val label = context.labelGetForName("Person")

        val constraints = context.constraintsGetForSchema(SchemaDescriptorFactory.forLabel(label, prop)).asScala

        constraints should have size 1
    }
  }

  test("should_add_constraint_using_recreated_unique_data") {
    // GIVEN
    execute("create (a:Person{name:\"Alistair\"}), (b:Person{name:\"Stefan\"})")
    execute("match (n:Person) delete n")
    execute("create (a:Person{name:\"Alistair\"}), (b:Person{name:\"Stefan\"})")

    // WHEN
    execute("create constraint on (n:Person) assert n.name is unique")

    // THEN
    graph.inTx {
      context: OperationsFacade =>
        val prop = context.propertyKeyGetForName("name")
        val label = context.labelGetForName("Person")

        val constraints = context.constraintsGetForSchema(SchemaDescriptorFactory.forLabel(label, prop)).asScala

        constraints should have size 1
    }
  }

  test("should_drop_constraint") {
    //GIVEN
    execute("create constraint on (node:Label) assert node.propertyKey is unique")

    //WHEN
    execute("drop constraint on (node:Label) assert node.propertyKey is unique")

    //THEN
    graph.inTx {
      context: OperationsFacade =>
        val prop = context.propertyKeyGetForName("propertyKey")
        val label = context.labelGetForName("Label")

        val constraints = context.constraintsGetForSchema(SchemaDescriptorFactory.forLabel(label, prop)).asScala

        constraints shouldBe empty
    }
  }

  test("should_fail_to_add_constraint_when_existing_data_conflicts") {
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
      context: OperationsFacade =>
        val prop = context.propertyKeyGetForName("id")
        val label = context.labelGetForName("Person")

        val constraints = context.constraintsGetForSchema(SchemaDescriptorFactory.forLabel(label, prop)).asScala

        constraints shouldBe empty
    }
  }
}
