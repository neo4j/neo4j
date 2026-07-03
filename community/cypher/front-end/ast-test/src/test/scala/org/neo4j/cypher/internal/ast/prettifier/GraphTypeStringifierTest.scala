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
package org.neo4j.cypher.internal.ast.prettifier

import org.neo4j.cypher.internal.ast.AstGraphTypeConstructionTestSupport
import org.neo4j.cypher.internal.ast.GraphType
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.graphtype.GraphTypeTestCase
import org.neo4j.cypher.internal.util.symbols.DateType
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.Integer32Type
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.symbols.VectorType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe
import org.scalatest.Assertion

import scala.collection.immutable.ArraySeq

class GraphTypeStringifierTest extends CypherFunSuite with AstGraphTypeConstructionTestSupport {

  test("Running example RE-1 canonicalised") {
    graphType(
      Seq(
        nodeType("City", Set("Location"), propertyType("name", StringType(isNullable = true))),
        nodeType("Site", Set("Location"), propertyType("name", StringType(isNullable = true))),
        nodeType(
          "Student",
          Set("Person"),
          propertyType("name", StringType(isNullable = false)),
          propertyType("studId", IntegerType(isNullable = true)),
          propertyType("birthday", DateType(isNullable = true))
        ),
        edgeType(identifyingNodeTypeRef("Student"), "LIVES_IN", identifyingNodeTypeRef("City")),
        edgeType(identifyingNodeTypeRef("Student"), "VISITED", nodeTypeRefByLabel("Location"))
      ),
      Seq(
        keyConstraint(identifyingNodeTypeRef("City", "n"), ArraySeq(prop(varFor("n"), "name"))),
        keyConstraint("mySiteConstraint", identifyingNodeTypeRef("Site", "n"), ArraySeq(prop(varFor("n"), "name"))),
        uniquenessConstraint(
          identifyingNodeTypeRef("Student", "n"),
          ArraySeq(prop(varFor("n"), "name"), prop(varFor("n"), "birthday")),
          OptionsMap(Map("indexProvider" -> literalString("range-1.0")))(pos)
        ),
        keyConstraint(identifyingNodeTypeRef("Student", "n"), ArraySeq(prop(varFor("n"), "studId"))),
        propertyTypeConstraint(
          nodeTypeRefByLabel("Person", "n"),
          ArraySeq(prop(varFor("n"), "age")),
          IntegerType(isNullable = true)(defaultPos)
        ),
        uniquenessConstraint(edgeTypeRefByLabel("LegacyRel", "r"), ArraySeq(prop(varFor("r"), "foo")))
      )
    ) shouldStringifyTo
      """{
        |  (:`City` => :`Location` {`name` :: STRING}),
        |  (:`Site` => :`Location` {`name` :: STRING}),
        |  (:`Student` => :`Person` {`birthday` :: DATE, `name` :: STRING NOT NULL, `studId` :: INTEGER}),
        |  (:`Student` =>)-[:`LIVES_IN` =>]->(:`City` =>),
        |  (:`Student` =>)-[:`VISITED` =>]->(:`Location`),
        |  CONSTRAINT FOR (`n`:`City` =>) REQUIRE (`n`.`name`) IS KEY,
        |  CONSTRAINT `mySiteConstraint` FOR (`n`:`Site` =>) REQUIRE (`n`.`name`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Student` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS UNIQUE OPTIONS {`indexProvider`: "range-1.0"},
        |  CONSTRAINT FOR (`n`:`Student` =>) REQUIRE (`n`.`studId`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person`) REQUIRE (`n`.`age`) IS :: INTEGER,
        |  CONSTRAINT FOR ()-[`r`:`LegacyRel`]->() REQUIRE (`r`.`foo`) IS UNIQUE
        |}""".stripMargin
  }

  test("invalid constraint with no alias var") {
    graphType(
      Seq(),
      Seq(
        uniquenessConstraint(nodeTypeRefByLabel("City"), ArraySeq(prop(varFor("n"), "name"))),
        keyConstraint(identifyingNodeTypeRef("Person"), ArraySeq(prop(varFor("p"), "name"))),
        keyConstraint(identifyingNodeTypeRef("Person"), ArraySeq(prop(varFor("p"), "name"))),
        keyConstraint(edgeTypeRefByLabel("REL1"), ArraySeq(prop(varFor("r"), "name"))),
        uniquenessConstraint(identifyingEdgeTypeRef("REL2"), ArraySeq(prop(varFor("r2"), "name")))
      )
    ) shouldStringifyTo
      """{
        |  CONSTRAINT FOR (:`Person` =>) REQUIRE (`p`.`name`) IS KEY,
        |  CONSTRAINT FOR (:`City`) REQUIRE (`n`.`name`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[:`REL2` =>]->() REQUIRE (`r2`.`name`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[:`REL1`]->() REQUIRE (`r`.`name`) IS KEY
        |}""".stripMargin
  }

  test("sort order for refs should be identifying label, variable, label") {
    graphType(
      Seq(),
      Seq(
        keyConstraint(nodeTypeRefByVar("Person"), ArraySeq(prop(varFor("Person"), "name"))),
        keyConstraint(nodeTypeRefByLabel("Person", "Person"), ArraySeq(prop(varFor("Person"), "name"))),
        keyConstraint(identifyingNodeTypeRef("Person", "Person"), ArraySeq(prop(varFor("Person"), "name")))
      )
    ) shouldStringifyTo
      """{
        |  CONSTRAINT FOR (`Person`:`Person` =>) REQUIRE (`Person`.`name`) IS KEY,
        |  CONSTRAINT FOR (`Person`) REQUIRE (`Person`.`name`) IS KEY,
        |  CONSTRAINT FOR (`Person`:`Person`) REQUIRE (`Person`.`name`) IS KEY
        |}""".stripMargin
  }

  test("sort order for properties lists of differing lengths") {
    graphType(
      Seq(),
      Seq(
        keyConstraint(
          identifyingNodeTypeRef("L4", "n"),
          ArraySeq(prop(varFor("n"), "p1"), prop(varFor("n"), "p2"), prop(varFor("n"), "p3"))
        ),
        uniquenessConstraint(
          identifyingNodeTypeRef("L4", "n"),
          ArraySeq(prop(varFor("n"), "p1"), prop(varFor("n"), "p2"))
        ),
        keyConstraint(identifyingNodeTypeRef("L4", "n"), ArraySeq(prop(varFor("n"), "p2"), prop(varFor("n"), "p1")))
      )
    ) shouldStringifyTo
      """{
        |  CONSTRAINT FOR (`n`:`L4` =>) REQUIRE (`n`.`p1`, `n`.`p2`) IS UNIQUE,
        |  CONSTRAINT FOR (`n`:`L4` =>) REQUIRE (`n`.`p1`, `n`.`p2`, `n`.`p3`) IS KEY,
        |  CONSTRAINT FOR (`n`:`L4` =>) REQUIRE (`n`.`p2`, `n`.`p1`) IS KEY
        |}""".stripMargin
  }

  test("graph types with inner vector type should be normalized") {
    graphType(
      Seq(
        nodeType(
          "Product",
          "p",
          propertyType("feature", VectorType(Some(FloatType(isNullable = false)(pos)), Some(4), isNullable = true))
        )
      ),
      Seq(
        propertyTypeConstraint(
          "myVectorConstr",
          edgeTypeRefByLabel("CONNECTION", "r"),
          ArraySeq(prop("r", "score")),
          VectorType(Some(Integer32Type(isNullable = false)(defaultPos)), Some(3), isNullable = true)(defaultPos)
        )
      )
    ) shouldStringifyTo
      """{
        |  (`p`:`Product` => {`feature` :: VECTOR<FLOAT NOT NULL>(4)}),
        |  CONSTRAINT `myVectorConstr` FOR ()-[`r`:`CONNECTION`]->() REQUIRE (`r`.`score`) IS :: VECTOR<INTEGER32 NOT NULL>(3)
        |}""".stripMargin
  }

  test("should stringify multiple independent constraints in sorted order") {
    /*
      {
          CONSTRAINT `cst1` FOR (`n`:`Person`) REQUIRE (`n`.`style`) IS UNIQUE,
          CONSTRAINT `cst2` FOR (`n`:`Person`) REQUIRE (`n`.`id`) IS UNIQUE,
          CONSTRAINT `cst3` FOR (`n`:`Person`) REQUIRE (`n`.`age`) IS NOT NULL,
          CONSTRAINT `cst4` FOR (`n`:`Person`) REQUIRE (`n`.`id`) IS KEY
      }
     */
    graphType(
      Seq(),
      Seq(
        uniquenessConstraint(
          "cst1",
          nodeTypeRefByLabel("Person", "n"),
          ArraySeq(
            prop(varFor("n"), "style")
          )
        ),
        uniquenessConstraint(
          "cst2",
          nodeTypeRefByLabel("Person", "n"),
          ArraySeq(
            prop(varFor("n"), "id")
          )
        ),
        existsConstraint(
          "cst3",
          nodeTypeRefByLabel("Person", "n"),
          ArraySeq(
            prop(varFor("n"), "age")
          )
        ),
        keyConstraint(
          "cst4",
          nodeTypeRefByLabel("Person", "n"),
          ArraySeq(
            prop(varFor("n"), "id")
          )
        )
      )
    ) shouldStringifyTo
      """{
        |  CONSTRAINT `cst3` FOR (`n`:`Person`) REQUIRE (`n`.`age`) IS NOT NULL,
        |  CONSTRAINT `cst4` FOR (`n`:`Person`) REQUIRE (`n`.`id`) IS KEY,
        |  CONSTRAINT `cst2` FOR (`n`:`Person`) REQUIRE (`n`.`id`) IS UNIQUE,
        |  CONSTRAINT `cst1` FOR (`n`:`Person`) REQUIRE (`n`.`style`) IS UNIQUE
        |}""".stripMargin
  }

  GraphTypeTestCase.testcases.collect { case GraphTypeTestCase(name, _, ast, prettifiedCypher, _) =>
    test(name) {
      ast shouldStringifyTo prettifiedCypher
    }
  }

  implicit private class GraphTypeMatchers(graphType: GraphType) {

    implicit val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

    def shouldStringifyTo(expected: String): Assertion = {
      // using `equal` instead of `be` for the windows line endings
      GraphTypeStringifier.apply(graphType) should equal(expected)
    }
  }

}
