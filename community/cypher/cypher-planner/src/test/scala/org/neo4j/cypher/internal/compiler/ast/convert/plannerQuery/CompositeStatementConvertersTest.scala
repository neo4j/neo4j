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
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.cypher.internal.ast.GraphDirectReference
import org.neo4j.cypher.internal.ast.Union.UnionMapping
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.CallSubqueryHorizon
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryProjection
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.RunQueryAtProjection
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.ir.UnwindProjection
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.InterestingOrderCandidate
import org.neo4j.cypher.internal.ir.ordering.RequiredOrderCandidate
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

/**
 * USE clauses behave differently depending the semantic features at hand:
 *   - Both 'USE multiple graph selector' and 'USE single graph selector' -> this combination is not allowed
 *   - 'USE multiple graph selector' -> USE clauses denote composite queries that should get packaged up inside [[RunQueryAtProjection]]s
 *   - 'USE single graph selector' -> USE clauses have already been handled by the query router and are essentially ignored at this point, see [[StandardStatementConvertersTest]]
 *   - otherwise -> USE clauses are not allowed
 */
class CompositeStatementConvertersTest extends CypherFunSuite with LogicalPlanningTestSupport {

  override val semanticFeatures: List[SemanticFeature] = List(
    SemanticFeature.UseAsMultipleGraphsSelector
  )

  test("standard single query") {
    val query =
      buildPlannerQuery(
        """MATCH (product: Product)
          |RETURN product""".stripMargin
      )

    query shouldEqual StandardFixtures.topLevelQuery
  }

  test("simple top-level composite query") {
    val query =
      buildPlannerQuery(
        """USE db.products
          |MATCH (product: Product)
          |RETURN product""".stripMargin
      )

    val expected =
      SinglePlannerQuery
        .empty
        .withHorizon(RunQueryAtProjection(
          graphReference = GraphDirectReference(CatalogName(List("db", "products")))(pos),
          queryString = """MATCH (`product`)
                          |  WHERE (`product`):`Product`
                          |RETURN `product` AS `product`""".stripMargin,
          parameters = Map.empty,
          columns = Set(varFor("product"))
        ))

    query shouldEqual expected
  }

  test("top-level composite query containing multiple clauses including a complex RETURN clause") {
    val query =
      buildPlannerQuery(
        """USE db.products
          |UNWIND [1,2,3] AS i
          |MATCH (product: Product)
          |RETURN DISTINCT product.code AS code ORDER BY code SKIP 5 LIMIT 10""".stripMargin
      )

    val expected =
      SinglePlannerQuery
        .empty
        .withHorizon(RunQueryAtProjection(
          graphReference = GraphDirectReference(CatalogName(List("db", "products")))(pos),
          queryString = """UNWIND [1, 2, 3] AS `i`
                          |MATCH (`product`)
                          |  WHERE (`product`):`Product`
                          |RETURN DISTINCT (`product`).`code` AS `code`
                          |  ORDER BY `code` ASCENDING
                          |  SKIP 5
                          |  LIMIT 10""".stripMargin,
          parameters = Map.empty,
          columns = Set(varFor("code"))
        ))

    query shouldEqual expected
  }

  test("standard union query") {
    val query =
      buildPlannerQuery(
        """MATCH (product: Product)
          |RETURN product
          |UNION
          |MATCH (product: Product {deleted: false})
          |RETURN product""".stripMargin
      )

    val expectedFirst =
      SinglePlannerQuery
        .empty
        .withQueryGraph(
          QueryGraph
            .empty
            .addPatternNodes(varFor("product"))
            .addPredicates(hasLabels("product", "Product"))
        ).withHorizon(RegularQueryProjection(
          projections = Map(varFor("product") -> varFor("product")),
          position = QueryProjection.Position.Intermediate
        ))

    val expectedSecond =
      SinglePlannerQuery
        .empty
        .withQueryGraph(
          QueryGraph
            .empty
            .addPatternNodes(varFor("product"))
            .addPredicates(hasLabels("product", "Product"))
            .addPredicates(in(prop("product", "deleted"), listOfBoolean(false)))
        ).withHorizon(RegularQueryProjection(
          projections = Map(varFor("product") -> varFor("product")),
          position = QueryProjection.Position.Intermediate
        ))

    val expected =
      UnionQuery(
        lhs = expectedFirst,
        rhs = expectedSecond,
        distinct = true,
        unionMappings = List(UnionMapping(varFor("product"), varFor("product"), varFor("product")))
      )

    query shouldEqual expected
  }

  test("composite union query") {
    val query =
      buildPlannerQuery(
        """USE db.products
          |MATCH (product: Product)
          |RETURN product
          |UNION ALL
          |UNWIND [1,2,3] AS i
          |RETURN i AS product
          |UNION ALL
          |USE db.products_bis
          |MATCH (product: Product {deleted: false})
          |RETURN product""".stripMargin
      )

    val expectedFirst =
      SinglePlannerQuery
        .empty
        .withHorizon(RunQueryAtProjection(
          graphReference = GraphDirectReference(CatalogName(List("db", "products")))(pos),
          queryString = """MATCH (`product`)
                          |  WHERE (`product`):`Product`
                          |RETURN `product` AS `product`""".stripMargin,
          parameters = Map.empty,
          columns = Set(varFor("product"))
        ))

    val expectedSecond =
      SinglePlannerQuery
        .empty
        .withHorizon(UnwindProjection(varFor("i"), listOfInt(1, 2, 3)))
        .withTail(
          SinglePlannerQuery
            .empty
            .withQueryGraph(
              QueryGraph
                .empty
                .addArgumentId(varFor("i"))
            ).withHorizon(RegularQueryProjection(
              projections = Map(varFor("product") -> varFor("i"))
            ))
        )

    val expectedThird =
      SinglePlannerQuery
        .empty
        .withHorizon(RunQueryAtProjection(
          graphReference = GraphDirectReference(CatalogName(List("db", "products_bis")))(pos),
          queryString = """MATCH (`product`)
                          |  WHERE (((`product`).`deleted`) IN ([false])) AND ((`product`):`Product`)
                          |RETURN `product` AS `product`""".stripMargin,
          parameters = Map.empty,
          columns = Set(varFor("product"))
        ))

    query shouldEqual
      UnionQuery(
        lhs = UnionQuery(
          lhs = expectedFirst,
          rhs = expectedSecond,
          distinct = false,
          unionMappings = List(UnionMapping(varFor("product"), varFor("product"), varFor("product")))
        ),
        rhs = expectedThird,
        distinct = false,
        unionMappings = List(UnionMapping(varFor("product"), varFor("product"), varFor("product")))
      )
  }

  test("standard query containing a standard sub-query") {
    val query =
      buildPlannerQuery(
        """MATCH (product: Product)
          |WITH product, product.id AS pId
          |CALL {
          |  WITH pId
          |  MATCH (customer)-[bought]->(product {id: pId})
          |  RETURN customer
          |}
          |RETURN product, customer""".stripMargin
      )

    query shouldEqual StandardFixtures.queryContainingSubQuery
  }

  test("standard query containing a simple composite sub-query") {
    val query =
      buildPlannerQuery(
        """MATCH (product: Product)
          |WITH product, product.id AS pId
          |CALL {
          |  WITH pId
          |  USE db.customers
          |  MATCH (customer)-[bought]->(product {id: pId})
          |  RETURN customer
          |}
          |RETURN product, customer""".stripMargin
      )

    query shouldEqual productQueryWithCustomerSubQuery
  }

  test("standard query containing a simple composite sub-query – import WITH after USE") {
    val query =
      buildPlannerQuery(
        """MATCH (product: Product)
          |WITH product, product.id AS pId
          |CALL {
          |  USE db.customers
          |  WITH pId // Note how the importing WITH clause is placed _after_ the USE clause
          |  MATCH (customer)-[bought]->(product {id: pId})
          |  RETURN customer
          |}
          |RETURN product, customer""".stripMargin
      )

    query shouldEqual productQueryWithCustomerSubQuery
  }

  test("query containing a simple composite sub-query and a union sub-query with composite constituents") {
    val query =
      buildPlannerQuery(
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
          |    MATCH (customer)-[bought]->(product {id: pId})
          |    RETURN customer
          |  UNION
          |    UNWIND [1,2,3] AS i
          |    WITH {id: i} AS customer
          |    RETURN customer
          |  UNION
          |    USE db.customerEU
          |    WITH pId
          |    MATCH (customer)-[bought]->(product {id: pId})
          |    RETURN customer
          |}
          |RETURN product, customer""".stripMargin
      )

    val expected =
      SinglePlannerQuery
        .empty
        .withHorizon(UnwindProjection(varFor("i"), listOfInt(1, 2, 3)))
        .withTail(
          SinglePlannerQuery
            .empty
            .withQueryGraph(
              QueryGraph
                .empty
                .addArgumentId(varFor("i"))
            ).withInterestingOrder(
              InterestingOrder.interested(InterestingOrderCandidate(List(
                ColumnOrder.Asc(prop("product", "name"), Map(varFor("product") -> varFor("product")))
              )))
            ).withHorizon(CallSubqueryHorizon(
              callSubquery =
                SinglePlannerQuery
                  .empty
                  .withQueryGraph(
                    QueryGraph
                      .empty
                      .addArgumentId(varFor("i"))
                  ).withHorizon(RunQueryAtProjection(
                    graphReference = GraphDirectReference(CatalogName(List("db", "products")))(pos),
                    queryString =
                      """WITH $`i` AS `i`
                        |MATCH (`product`)
                        |  WHERE (((`product`).`version`) IN ([`i`])) AND ((`product`):`Product`)
                        |RETURN `product` AS `product`""".stripMargin,
                    parameters = Map(parameter("i", CTAny) -> varFor("i")),
                    columns = Set(varFor("product"))
                  )),
              correlated = true,
              yielding = true,
              inTransactionsParameters = None
            )).withTail(
              SinglePlannerQuery
                .empty
                .withQueryGraph(
                  QueryGraph
                    .empty
                    .addArgumentIds(List(varFor("i"), varFor("product")))
                ).withInterestingOrder(
                  InterestingOrder.required(RequiredOrderCandidate(List(
                    ColumnOrder.Asc(prop("product", "name"), Map(varFor("product") -> varFor("product")))
                  )))
                ).withHorizon(RegularQueryProjection(
                  projections = Map(varFor("i") -> varFor("i"), varFor("product") -> varFor("product")),
                  position = QueryProjection.Position.Intermediate
                )).withTail(
                  SinglePlannerQuery
                    .empty
                    .withQueryGraph(
                      QueryGraph
                        .empty
                        .addArgumentIds(List(varFor("i"), varFor("product")))
                    ).withHorizon(RegularQueryProjection(
                      projections = Map(varFor("product") -> varFor("product"), varFor("pId") -> prop("product", "id")),
                      position = QueryProjection.Position.Intermediate
                    )).withTail(
                      SinglePlannerQuery
                        .empty
                        .withQueryGraph(
                          QueryGraph
                            .empty
                            .addArgumentIds(List(varFor("product"), varFor("pId")))
                        ).withHorizon(
                          CallSubqueryHorizon(
                            callSubquery = UnionQuery(
                              lhs = UnionQuery(
                                lhs =
                                  SinglePlannerQuery
                                    .empty
                                    .withQueryGraph(
                                      QueryGraph
                                        .empty
                                        .addArgumentId(varFor("pId"))
                                    ).withHorizon(RunQueryAtProjection(
                                      graphReference =
                                        GraphDirectReference(CatalogName(List("db", "customerAME")))(pos),
                                      queryString =
                                        """WITH $`pId` AS `pId`
                                          |MATCH (`customer`)-[`bought`]->(`product`)
                                          |  WHERE ((`product`).`id`) IN ([`pId`])
                                          |RETURN `customer` AS `customer`""".stripMargin,
                                      parameters = Map(parameter("pId", CTAny) -> varFor("pId")),
                                      columns = Set(varFor("customer"))
                                    )),
                                rhs =
                                  SinglePlannerQuery
                                    .empty
                                    .withHorizon(UnwindProjection(varFor("i"), listOfInt(1, 2, 3)))
                                    .withTail(
                                      SinglePlannerQuery
                                        .empty
                                        .withQueryGraph(
                                          QueryGraph
                                            .empty
                                            .addArgumentId(varFor("i"))
                                        ).withHorizon(RegularQueryProjection(
                                          projections = Map(varFor("customer") -> mapOf("id" -> varFor("i"))),
                                          position = QueryProjection.Position.Intermediate
                                        )).withTail(
                                          SinglePlannerQuery
                                            .empty
                                            .withQueryGraph(
                                              QueryGraph
                                                .empty
                                                .addArgumentId(varFor("customer"))
                                            ).withHorizon(RegularQueryProjection(
                                              projections = Map(varFor("customer") -> varFor("customer")),
                                              position = QueryProjection.Position.Intermediate
                                            ))
                                        )
                                    ),
                                distinct = true,
                                unionMappings =
                                  List(UnionMapping(varFor("customer"), varFor("customer"), varFor("customer")))
                              ),
                              rhs =
                                SinglePlannerQuery
                                  .empty
                                  .withQueryGraph(
                                    QueryGraph
                                      .empty
                                      .addArgumentId(varFor("pId"))
                                  ).withHorizon(RunQueryAtProjection(
                                    graphReference = GraphDirectReference(CatalogName(List("db", "customerEU")))(pos),
                                    queryString =
                                      """WITH $`pId` AS `pId`
                                        |MATCH (`customer`)-[`bought`]->(`product`)
                                        |  WHERE ((`product`).`id`) IN ([`pId`])
                                        |RETURN `customer` AS `customer`""".stripMargin,
                                    parameters = Map(parameter("pId", CTAny) -> varFor("pId")),
                                    columns = Set(varFor("customer"))
                                  )),
                              distinct = true,
                              unionMappings =
                                List(UnionMapping(varFor("customer"), varFor("customer"), varFor("customer")))
                            ),
                            correlated = true,
                            yielding = true,
                            inTransactionsParameters = None
                          )
                        ).withTail(
                          SinglePlannerQuery
                            .empty
                            .withQueryGraph(
                              QueryGraph
                                .empty
                                .addArgumentIds(List(varFor("product"), varFor("pId"), varFor("customer")))
                            ).withHorizon(RegularQueryProjection(
                              projections =
                                Map(varFor("product") -> varFor("product"), varFor("customer") -> varFor("customer")),
                              position = QueryProjection.Position.Final
                            ))
                        )
                    )
                )
            )
        )

    query shouldEqual expected
  }

  private lazy val productQueryWithCustomerSubQuery =
    SinglePlannerQuery
      .empty
      .withQueryGraph(
        QueryGraph
          .empty
          .addPatternNodes(varFor("product"))
          .addPredicates(hasLabels("product", "Product"))
      ).withHorizon(RegularQueryProjection(
        projections = Map(varFor("product") -> varFor("product"), varFor("pId") -> prop("product", "id")),
        position = QueryProjection.Position.Intermediate
      )).withTail(
        SinglePlannerQuery
          .empty
          .withQueryGraph(
            QueryGraph
              .empty
              .addArgumentIds(List(varFor("product"), varFor("pId")))
          ).withHorizon(CallSubqueryHorizon(
            callSubquery = RegularSinglePlannerQuery(
              queryGraph =
                QueryGraph
                  .empty
                  .addArgumentId(varFor("pId")),
              horizon =
                RunQueryAtProjection(
                  graphReference = GraphDirectReference(CatalogName(List("db", "customers")))(pos),
                  queryString =
                    """WITH $`pId` AS `pId`
                      |MATCH (`customer`)-[`bought`]->(`product`)
                      |  WHERE ((`product`).`id`) IN ([`pId`])
                      |RETURN `customer` AS `customer`""".stripMargin,
                  parameters = Map(parameter("pId", CTAny) -> varFor("pId")),
                  columns = Set(varFor("customer"))
                )
            ),
            correlated = true,
            yielding = true,
            inTransactionsParameters = None
          )).withTail(
            SinglePlannerQuery
              .empty
              .withQueryGraph(
                QueryGraph
                  .empty
                  .addArgumentIds(List(varFor("product"), varFor("pId"), varFor("customer")))
              ).withHorizon(RegularQueryProjection(
                projections = Map(varFor("product") -> varFor("product"), varFor("customer") -> varFor("customer")),
                position = QueryProjection.Position.Final
              ))
          )
      )
}

class StandardStatementConvertersTest extends CypherFunSuite with LogicalPlanningTestSupport {

  override val semanticFeatures: List[SemanticFeature] = List(
    SemanticFeature.UseAsSingleGraphSelector
  )

  test("USE in a top-level query, with 'USE as single graph selector'") {
    val query =
      buildPlannerQuery(
        """USE db.products
          |MATCH (product: Product)
          |RETURN product""".stripMargin
      )

    query shouldEqual StandardFixtures.topLevelQuery
  }

  test("USE in a sub-query, with 'USE as single graph selector'") {
    val query =
      buildPlannerQuery(
        """MATCH (product: Product)
          |WITH product, product.id AS pId
          |CALL {
          |  WITH pId
          |  USE db.customers
          |  MATCH (customer)-[bought]->(product {id: pId})
          |  RETURN customer
          |}
          |RETURN product, customer""".stripMargin
      )

    query shouldEqual StandardFixtures.queryContainingSubQuery
  }
}

object StandardFixtures extends AstConstructionTestSupport {

  val topLevelQuery: PlannerQuery =
    SinglePlannerQuery
      .empty
      .withQueryGraph(
        QueryGraph
          .empty
          .addPatternNodes(varFor("product"))
          .addPredicates(hasLabels("product", "Product"))
      ).withHorizon(RegularQueryProjection(
        projections = Map(varFor("product") -> varFor("product")),
        position = QueryProjection.Position.Final
      ))

  val queryContainingSubQuery: PlannerQuery =
    SinglePlannerQuery
      .empty
      .withQueryGraph(
        QueryGraph
          .empty
          .addPatternNodes(varFor("product"))
          .addPredicates(hasLabels("product", "Product"))
      ).withHorizon(RegularQueryProjection(
        projections = Map(varFor("product") -> varFor("product"), varFor("pId") -> prop("product", "id")),
        position = QueryProjection.Position.Intermediate
      )).withTail(
        SinglePlannerQuery
          .empty
          .withQueryGraph(
            QueryGraph
              .empty
              .addArgumentIds(List(varFor("product"), varFor("pId")))
          ).withHorizon(CallSubqueryHorizon(
            callSubquery =
              SinglePlannerQuery
                .empty
                .withQueryGraph(
                  QueryGraph
                    .empty
                    .addArgumentId(varFor("pId"))
                    .addPatternNodes(
                      varFor("customer"),
                      varFor("product")
                    )
                    .addPatternRelationship(PatternRelationship(
                      variable = varFor("bought"),
                      boundaryNodes = (varFor("customer"), varFor("product")),
                      dir = SemanticDirection.OUTGOING,
                      types = Nil,
                      length = SimplePatternLength
                    ))
                    .addPredicates(
                      in(prop("product", "id"), listOf(varFor("pId")))
                    )
                ).withHorizon(RegularQueryProjection(
                  projections = Map(varFor("customer") -> varFor("customer")),
                  position = QueryProjection.Position.Intermediate
                )),
            correlated = true,
            yielding = true,
            inTransactionsParameters = None
          )).withTail(
            SinglePlannerQuery
              .empty
              .withQueryGraph(
                QueryGraph
                  .empty
                  .addArgumentIds(List(varFor("product"), varFor("pId"), varFor("customer")))
              ).withHorizon(RegularQueryProjection(
                projections = Map(varFor("product") -> varFor("product"), varFor("customer") -> varFor("customer")),
                position = QueryProjection.Position.Final
              ))
          )
      )
}
