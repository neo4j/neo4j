/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.mockito.Mockito.when
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.database.NormalizedDatabaseName

class CompositeQueryPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport {

  final private val NL: String = System.lineSeparator()

  final private val productsDatabaseReference = mock[DatabaseReference]

  when(productsDatabaseReference.fullName())
    .thenReturn(new NormalizedDatabaseName("db.products"))

  final private val customersDatabaseReference = mock[DatabaseReference]

  when(customersDatabaseReference.fullName())
    .thenReturn(new NormalizedDatabaseName("db.customers"))

  final private val compositeDatabaseReference = mock[DatabaseReferenceImpl.Composite]

  when(compositeDatabaseReference.constituents())
    .thenReturn(java.util.List.of(productsDatabaseReference, customersDatabaseReference))

  final private val databaseReferenceRepository = mock[DatabaseReferenceRepository]

  when(databaseReferenceRepository.getCompositeDatabaseReferences)
    .thenReturn(java.util.Set.of(compositeDatabaseReference))

  final private val planner =
    plannerBuilder()
      .addSemanticFeature(SemanticFeature.UseAsMultipleGraphsSelector)
      .withSetting(GraphDatabaseInternalSettings.composite_queries_with_query_router, Boolean.box(true))
      .setDatabaseReferenceRepository(databaseReferenceRepository)
      .setAllNodesCardinality(0)
      .build()

  test("should plan a simple composite query with a MATCH clause") {
    val query =
      """USE db.products
        |MATCH (product: Product)
        |RETURN product""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("product")
      .runQueryAt(
        query = List(
          "MATCH (`product`)",
          "  WHERE (`product`):`Product`",
          "RETURN `product` AS `product`"
        ).mkString(NL),
        graphReference = "db.products",
        columns = Set("product")
      )
      .argument()
      .build()
  }

  test("should plan a simple composite query with a MATCH clause returning a complex expression") {
    val query =
      """USE db.products
        |MATCH (product: Product)
        |RETURN DISTINCT product.code + '#' + product.version AS code ORDER BY code SKIP 2 LIMIT 20""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("code")
      .runQueryAt(
        query = List(
          "MATCH (`product`)",
          "  WHERE (`product`):`Product`",
          "RETURN DISTINCT (((`product`).`code`) + (\"#\")) + ((`product`).`version`) AS `code`",
          "  ORDER BY `code` ASCENDING",
          "  SKIP 2",
          "  LIMIT 20"
        ).mkString(NL),
        graphReference = "db.products",
        columns = Set("code")
      )
      .argument()
      .build()
  }

  test("should plan a simple composite query with a DELETE clause") {
    val query =
      """USE db.products
        |MATCH (product: Product)
        |DELETE product""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults()
      .runQueryAt(
        query = List(
          "MATCH (`product`)",
          "  WHERE (`product`):`Product`",
          "DELETE `product`"
        ).mkString(NL),
        graphReference = "db.products"
      )
      .argument()
      .build()
  }

  test("should plan the UNION of two simple composite queries") {
    val query =
      """USE db.products
        |MATCH (product: Product)
        |RETURN product
        |UNION
        |USE db.products_bis
        |MATCH (product: Product {deleted: false})
        |RETURN product""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("product")
      .distinct("product AS product")
      .union()
      .|.projection("product AS product")
      .|.runQueryAt(
        query = List(
          "MATCH (`product`)",
          "  WHERE (((`product`).`deleted`) IN ([false])) AND ((`product`):`Product`)",
          "RETURN `product` AS `product`"
        ).mkString(NL),
        graphReference = "db.products_bis",
        columns = Set("product")
      )
      .|.argument()
      .projection("product AS product")
      .runQueryAt(
        query = List(
          "MATCH (`product`)",
          "  WHERE (`product`):`Product`",
          "RETURN `product` AS `product`"
        ).mkString(NL),
        graphReference = "db.products",
        columns = Set("product")
      )
      .argument()
      .build()
  }

  test("should plan a composite query containing sub-queries") {
    val query =
      """UNWIND [1,2,3] AS i
        |CALL {
        |  WITH i
        |  USE db.products
        |  MATCH (product:Product {version: i})
        |  RETURN product
        |}
        |WITH * ORDER BY product.name
        |WITH product, product.id AS pId
        |CALL {
        |  USE db.customers
        |  WITH pId
        |  MATCH (customer:Customer)-[:BOUGHT]->(:Product {id: pId})
        |  RETURN customer
        |}
        |RETURN product, customer
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("product", "customer")
      .apply()
      .|.runQueryAt(
        query = List(
          "WITH $`pId` AS `pId`",
          "MATCH (`customer`)-[`anon_0`:`BOUGHT`]->(`anon_1`)",
          "  WHERE ((`customer`):`Customer`) AND (((`anon_1`).`id`) IN ([`pId`])) AND ((`anon_1`):`Product`)",
          "RETURN `customer` AS `customer`"
        ).mkString(NL),
        graphReference = "db.customers",
        parameters = Map("$pId" -> "pId"),
        columns = Set("customer")
      )
      .|.argument("pId")
      .projection("product.id AS pId")
      .sort("`product.name` ASC")
      .projection("product.name AS `product.name`")
      .apply()
      .|.runQueryAt(
        query = List(
          "WITH $`i` AS `i`",
          "MATCH (`product`)",
          "  WHERE (((`product`).`version`) IN ([`i`])) AND ((`product`):`Product`)",
          "RETURN `product` AS `product`"
        ).mkString(NL),
        graphReference = "db.products",
        parameters = Map("$i" -> "i"),
        columns = Set("product")
      )
      .|.argument("i")
      .unwind("[1, 2, 3] AS i")
      .argument()
      .build()
  }

  test("should plan a composite query containing union sub-queries") {
    val query =
      """UNWIND [1,2,3] AS i
        |CALL {
        |  WITH i
        |  USE db.products
        |  MATCH (product:Product {version: i})
        |  RETURN product
        |}
        |WITH * ORDER BY product.name
        |WITH product, product.id AS pId
        |CALL {
        |    WITH pId
        |    USE db.customerAME
        |    MATCH (customer:Customer)-[:BOUGHT]->(:Product {id: pId})
        |    RETURN customer
        |  UNION
        |    UNWIND [1,2,3] AS i
        |    WITH {id: i} AS customer
        |    RETURN customer
        |  UNION
        |    USE db.customerEU
        |    WITH pId
        |    MATCH (customer:Customer)-[:BOUGHT]->(:Product {id: pId})
        |    RETURN customer
        |}
        |RETURN product, customer
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("product", "customer")
      .apply()
      .|.distinct("customer AS customer")
      .|.union()
      .|.|.projection("customer AS customer")
      .|.|.runQueryAt(
        query = List(
          "WITH $`pId` AS `pId`",
          "MATCH (`customer`)-[`anon_2`:`BOUGHT`]->(`anon_3`)",
          "  WHERE ((`customer`):`Customer`) AND (((`anon_3`).`id`) IN ([`pId`])) AND ((`anon_3`):`Product`)",
          "RETURN `customer` AS `customer`"
        ).mkString(NL),
        graphReference = "db.customerEU",
        parameters = Map("$pId" -> "pId"),
        columns = Set("customer")
      )
      .|.|.argument("pId")
      .|.projection("customer AS customer")
      .|.union()
      .|.|.projection("customer AS customer")
      .|.|.projection("{id: i} AS customer")
      .|.|.unwind("[1, 2, 3] AS i")
      .|.|.argument()
      .|.projection("customer AS customer")
      .|.runQueryAt(
        query = List(
          "WITH $`pId` AS `pId`",
          "MATCH (`customer`)-[`anon_0`:`BOUGHT`]->(`anon_1`)",
          "  WHERE ((`customer`):`Customer`) AND (((`anon_1`).`id`) IN ([`pId`])) AND ((`anon_1`):`Product`)",
          "RETURN `customer` AS `customer`"
        ).mkString(NL),
        graphReference = "db.customerAME",
        parameters = Map("$pId" -> "pId"),
        columns = Set("customer")
      )
      .|.argument("pId")
      .projection("product.id AS pId")
      .sort("`product.name` ASC")
      .projection("product.name AS `product.name`")
      .apply()
      .|.runQueryAt(
        query = List(
          "WITH $`i` AS `i`",
          "MATCH (`product`)",
          "  WHERE (((`product`).`version`) IN ([`i`])) AND ((`product`):`Product`)",
          "RETURN `product` AS `product`"
        ).mkString(NL),
        graphReference = "db.products",
        parameters = Map("$i" -> "i"),
        columns = Set("product")
      )
      .|.argument("i")
      .unwind("[1, 2, 3] AS i")
      .argument()
      .build()
  }

  test("should plan a composite query where an imported variable name clashes with a parameter name") {
    val query =
      """UNWIND [1,2,3] AS i
        |CALL {
        |  WITH i
        |  USE db.products
        |  MATCH (product:Product {version: i, status: $i})
        |  RETURN product ORDER BY product.name
        |}
        |RETURN product
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("product")
      .apply()
      .|.runQueryAt(
        query = List(
          "WITH $`anon_0` AS `i`",
          "MATCH (`product`)",
          "  WHERE (((`product`).`version`) IN ([`i`])) AND (((`product`).`status`) IN ([$`i`])) AND ((`product`):`Product`)",
          "RETURN `product` AS `product`",
          "  ORDER BY (`product`).`name` ASCENDING"
        ).mkString(NL),
        graphReference = "db.products",
        parameters = Map("$anon_0" -> "i"),
        columns = Set("product")
      )
      .|.argument("i")
      .unwind("[1, 2, 3] AS i")
      .argument()
      .build()
  }

  test("should plan a composite query with a graph reference dependency with no importing WITH") {
    val query =
      """UNWIND ['db.customerAME', 'db.customerEU'] as component
        |CALL {
        |  USE graph.byName(component) // depends on `component` without importing it
        |  MATCH (n)
        |  RETURN n.prop as prop
        |}
        |RETURN prop
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("prop")
      .apply()
      .|.runQueryAt(
        query = List(
          "MATCH (`n`)",
          "RETURN (`n`).`prop` AS `prop`"
        ).mkString(NL),
        graphReference = "graph.byName(component)",
        columns = Set("prop")
      )
      .|.argument("component")
      .unwind("['db.customerAME', 'db.customerEU'] AS component")
      .argument()
      .build()
  }
}
