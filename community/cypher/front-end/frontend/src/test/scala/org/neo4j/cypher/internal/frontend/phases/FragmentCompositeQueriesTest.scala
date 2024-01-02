/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.cypher.internal.ast.GraphDirectReference
import org.neo4j.cypher.internal.ast.GraphReference
import org.neo4j.cypher.internal.ast.RunQueryAt
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

/**
 * USE clauses behave differently depending the semantic features at hand:
 *   - Both 'USE multiple graph selector' and 'USE single graph selector' -> this combination is not allowed
 *   - 'USE multiple graph selector' -> USE clauses should be rewritten by [[FragmentCompositeQueries]], where applicable, as covered by this test
 *   - 'USE single graph selector' -> USE clauses should never be rewritten by [[FragmentCompositeQueries]], see [[DoNotFragmentCompositeQueriesTest]]
 *   - otherwise -> USE clauses are not allowed
 */
class FragmentCompositeQueriesTest
    extends CypherFunSuite
    with AstConstructionTestSupport
    with RewritePhaseTest {

  override def semanticFeatures: Seq[SemanticFeature] =
    List(SemanticFeature.UseAsMultipleGraphsSelector)

  override def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState] =
    FragmentCompositeQueries(semanticFeatures.toSet)

  test("rewrites a simple top-level composite query") {
    assertRewritten(
      """USE db.products
        |MATCH (product: Product)
        |RETURN product""".stripMargin,
      singleQuery(
        RunQueryAt(
          graphReference = GraphDirectReference(CatalogName(List("db", "products")))(pos),
          innerQuery = singleQuery(
            match_(nodePat(name = Some("product")), where = Some(where(hasLabels("product", "Product")))),
            return_(aliasedReturnItem(varFor("product")))
          ),
          parameters = Map.empty
        )(pos),
        return_(aliasedReturnItem(varFor("product")))
      )
    )
  }

  test("doesn't rewrite a standard single query") {
    assertNotRewritten(
      """MATCH (product: Product)
        |RETURN product""".stripMargin
    )
  }

  test("rewrites a top-level composite query containing multiple clauses including a complex RETURN clause") {
    assertRewritten(
      """USE db.products
        |UNWIND [1,2,3] AS i
        |MATCH (product: Product)
        |RETURN DISTINCT product.code AS code ORDER BY code SKIP 5 LIMIT 10""".stripMargin,
      singleQuery(
        RunQueryAt(
          graphReference = GraphDirectReference(CatalogName(List("db", "products")))(pos),
          innerQuery = singleQuery(
            unwind(listOfInt(1, 2, 3), varFor("i")),
            match_(
              pattern = nodePat(name = Some("product")),
              where = Some(where(hasLabels("product", "Product")))
            ),
            returnDistinct(
              ob = orderBy(varFor("code").asc),
              skip = skip(5),
              limit = limit(10),
              items = aliasedReturnItem(prop("product", "code"), "code")
            )
          ),
          parameters = Map.empty
        )(pos),
        return_(aliasedReturnItem(varFor("code")))
      )
    )
  }

  test("doesn't rewrite a standard union query") {
    assertNotRewritten(
      """MATCH (product: Product)
        |RETURN product
        |UNION
        |MATCH (product: Product {deleted: false})
        |RETURN product""".stripMargin
    )
  }

  test("only rewrites the composite constituents of a union query") {
    assertRewritten(
      """USE db.products
        |MATCH (product: Product)
        |RETURN product
        |UNION
        |UNWIND [1,2,3] AS i
        |RETURN i AS product
        |UNION
        |USE db.products_bis
        |MATCH (product: Product {deleted: false})
        |RETURN product""".stripMargin,
      union(
        union(
          singleQuery(
            RunQueryAt(
              graphReference = GraphDirectReference(CatalogName(List("db", "products")))(pos),
              innerQuery = singleQuery(
                match_(nodePat(name = Some("product")), where = Some(where(hasLabels("product", "Product")))),
                return_(aliasedReturnItem(varFor("product")))
              ),
              parameters = Map.empty
            )(pos),
            return_(aliasedReturnItem(varFor("product")))
          ),
          singleQuery(
            unwind(listOfInt(1, 2, 3), varFor("i")),
            return_(aliasedReturnItem(varFor("i"), "product"))
          )
        ),
        singleQuery(
          RunQueryAt(
            graphReference = GraphDirectReference(CatalogName(List("db", "products_bis")))(pos),
            innerQuery = singleQuery(
              match_(
                nodePat(
                  name = Some("product")
                ),
                where = Some(where(and(
                  equals(prop("product", "deleted"), falseLiteral),
                  hasLabels("product", "Product")
                )))
              ),
              return_(aliasedReturnItem(varFor("product")))
            ),
            parameters = Map.empty
          )(pos),
          return_(aliasedReturnItem(varFor("product")))
        )
      )
    )
  }

  test("doesn't rewrite a standard query containing a standard sub-query") {
    assertNotRewritten(
      """MATCH (product: Product)
        |WITH product, product.id AS pId
        |CALL {
        |  WITH pId
        |  MATCH (customer:Customer)-[:BOUGHT]->(:Product {id: pId})
        |  RETURN customer
        |}
        |RETURN product, customer""".stripMargin
    )
  }

  private val productQueryWithCustomerSubquery: SingleQuery =
    singleQuery(
      match_(nodePat(name = Some("product")), where = Some(where(hasLabels("product", "Product")))),
      with_(
        aliasedReturnItem(varFor("product")),
        aliasedReturnItem(prop("product", "id"), "pId")
      ),
      subqueryCall(
        customerQuery(GraphDirectReference(CatalogName(List("db", "customers")))(pos))
      ),
      return_(
        aliasedReturnItem(varFor("product")),
        aliasedReturnItem(varFor("customer"))
      )
    )

  test("rewrites a query containing a simple composite sub-query") {
    assertRewritten(
      """MATCH (product: Product)
        |WITH product, product.id AS pId
        |CALL {
        |  WITH pId
        |  USE db.customers
        |  MATCH (customer)-[bought]->(product {id: pId})
        |  RETURN customer
        |}
        |RETURN product, customer""".stripMargin,
      productQueryWithCustomerSubquery
    )
  }

  test("rewrites a query containing a simple composite sub-query – import WITH after USE") {
    assertRewritten(
      """MATCH (product: Product)
        |WITH product, product.id AS pId
        |CALL {
        |  USE db.customers
        |  WITH pId
        |  MATCH (customer)-[bought]->(product {id: pId})
        |  RETURN customer
        |}
        |RETURN product, customer""".stripMargin,
      productQueryWithCustomerSubquery
    )
  }

  test("rewrites a query containing a simple composite sub-query and a union sub-query with composite constituents") {
    val queryString =
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

    val productQuery: SingleQuery =
      singleQuery(
        with_(aliasedReturnItem(varFor("i"))),
        RunQueryAt(
          graphReference = GraphDirectReference(CatalogName(List("db", "products")))(pos),
          innerQuery = singleQuery(
            with_(
              aliasedReturnItem(parameter("i", CTAny), "i")
            ),
            match_(
              nodePat(name = Some("product")),
              where = Some(where(and(
                equals(prop("product", "version"), varFor("i")),
                hasLabels("product", "Product")
              )))
            ),
            return_(aliasedReturnItem(varFor("product")))
          ),
          parameters = Map(parameter("i", CTAny) -> varFor("i"))
        )(pos),
        return_(aliasedReturnItem(varFor("product")))
      )

    assertRewritten(
      queryString,
      singleQuery(
        unwind(listOfInt(1, 2, 3), varFor("i")),
        subqueryCall(
          productQuery
        ),
        With(
          distinct = false,
          returnItems = returnItems(
            aliasedReturnItem(varFor("i")),
            aliasedReturnItem(varFor("product"))
          ),
          orderBy = Some(orderBy(prop("product", "name").asc)),
          skip = None,
          limit = None,
          where = None
        )(pos),
        with_(
          aliasedReturnItem(varFor("product")),
          aliasedReturnItem(prop("product", "id"), "pId")
        ),
        subqueryCall(
          union(
            union(
              customerQuery(GraphDirectReference(CatalogName(List("db", "customerAME")))(pos)),
              singleQuery(
                unwind(listOfInt(1, 2, 3), varFor("i")),
                with_(aliasedReturnItem(mapOf("id" -> varFor("i")), "customer")),
                return_(aliasedReturnItem(varFor("customer")))
              )
            ),
            customerQuery(GraphDirectReference(CatalogName(List("db", "customerEU")))(pos))
          )
        ),
        return_(
          aliasedReturnItem(varFor("product")),
          aliasedReturnItem(varFor("customer"))
        )
      )
    )
  }

  private def customerQuery(graphReference: GraphReference): SingleQuery =
    singleQuery(
      with_(aliasedReturnItem(varFor("pId"))),
      RunQueryAt(
        graphReference = graphReference,
        innerQuery = singleQuery(
          with_(
            aliasedReturnItem(parameter("pId", CTAny), "pId")
          ),
          match_(
            pattern = relationshipChain(
              nodePat(name = Some("customer")),
              relPat(name = Some("bought")),
              nodePat(name = Some("product"))
            ),
            where = Some(where(equals(prop("product", "id"), varFor("pId"))))
          ),
          return_(aliasedReturnItem(varFor("customer")))
        ),
        parameters = Map(parameter("pId", CTAny) -> varFor("pId"))
      )(pos),
      return_(aliasedReturnItem(varFor("customer")))
    )
}

/**
 * USE clauses should not be rewritten under 'USE single graph selector'
 */
class DoNotFragmentCompositeQueriesTest
    extends CypherFunSuite
    with AstConstructionTestSupport
    with RewritePhaseTest {

  override def semanticFeatures: Seq[SemanticFeature] =
    List(SemanticFeature.UseAsSingleGraphSelector)

  override def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState] =
    FragmentCompositeQueries(semanticFeatures.toSet)

  // rewrites a simple top-level composite query
  test("doesn't rewrite top-level composite queries under 'USE as single graph selector'") {
    assertNotRewritten(
      """USE db.products
        |MATCH (product: Product)
        |RETURN product""".stripMargin
    )
  }

  test("doesn't rewrite composite sub-queries under 'USE as single graph selector'") {
    assertNotRewritten(
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
  }
}
