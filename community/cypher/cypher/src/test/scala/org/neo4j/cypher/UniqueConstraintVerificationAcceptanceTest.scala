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
package org.neo4j.cypher

import org.neo4j.cypher.internal.compiler.v2_3.helpers.CollectionSupport

import collection.JavaConverters._
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException
import org.neo4j.kernel.impl.api.OperationsFacade

class UniqueConstraintVerificationAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CollectionSupport {

  test("should_add_constraint_with_no_existing_data") {
    //GIVEN

    //WHEN
    execute("create constraint on (node:Label) assert node.propertyKey is unique")

    //THEN
    graph.inTx {
      context: OperationsFacade =>
        val prop = context.propertyKeyGetForName("propertyKey")
        val label = context.labelGetForName("Label")

        val constraints = context.constraintsGetForLabelAndPropertyKey(label, prop).asScala

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

        val constraints = context.constraintsGetForLabelAndPropertyKey(label, prop).asScala

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

        val constraints = context.constraintsGetForLabelAndPropertyKey(label, prop).asScala

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

        val constraints = context.constraintsGetForLabelAndPropertyKey(label, prop).asScala

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

        val constraints = context.constraintsGetForLabelAndPropertyKey(label, prop).asScala

        constraints shouldBe empty
    }
  }
}
