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
package org.neo4j.cypher.internal.ast.factory.ddl

import org.neo4j.cypher.internal.ast.AstGraphTypeConstructionTestSupport
import org.neo4j.cypher.internal.ast.EdgeType
import org.neo4j.cypher.internal.ast.EmptyNodeTypeReference
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.KeyConstraint
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.UniquenessConstraint
import org.neo4j.cypher.internal.ast.GraphTypeConstraintDefinition
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NodeType
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.PropertyType.PropertyInlineKeyConstraint
import org.neo4j.cypher.internal.ast.PropertyType.PropertyInlineUniquenessConstraint
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.ddl.GraphTypeParserTest.cypher5Error
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.test.util.Parses
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.graphtype.GraphTypeTestCase
import org.neo4j.cypher.internal.util.symbols.BFloat16Type
import org.neo4j.cypher.internal.util.symbols.DateType
import org.neo4j.cypher.internal.util.symbols.Float16Type
import org.neo4j.cypher.internal.util.symbols.Float32Type
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.Integer32Type
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.symbols.VectorType
import org.neo4j.gqlstatus.GqlStatusInfoCodes

import scala.collection.immutable.ArraySeq

class GraphTypeParserTest extends AstParsingTestBase with AstGraphTypeConstructionTestSupport {

  test(
    """ALTER CURRENT GRAPH TYPE SET { }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(
          alterCurrentGraphTypeSet(graphType())
        )
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET { (:Person IMPLIES { name :: STRING } ) }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(
          alterCurrentGraphTypeSet(graphType(nodeType(
            "Person",
            propertyType(
              "name",
              StringType(isNullable = true)
            )
          )))
        )
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET { (:Person => :Student&Happy {name :: STRING NOT NULL, age :: INT, studentID :: STRING}) }"""
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeSet(
          graphType(nodeType(
            "Person",
            Set("Student", "Happy"),
            propertyType("name", StringType(isNullable = false)),
            propertyType("age", IntegerType(isNullable = true)),
            propertyType("studentID", StringType(isNullable = true))
          ))
        ))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET {(p :Person => {name :: STRING IS KEY }) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeSet(
          graphType(
            nodeType(
              "Person",
              "p",
              propertyType(
                "name",
                StringType(isNullable = true),
                PropertyInlineKeyConstraint()(defaultPos)
              )
            )
          )
        ))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET {(p :Person => {name :: STRING}) REQUIRE p.name IS KEY}""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeSet(
          graphType(
            Seq(nodeTypeWithConstraints(
              "Person",
              "p",
              Set(KeyConstraint(
                ArraySeq(prop("p", "name", defaultPos))
              )(defaultPos)),
              propertyType("name", StringType(isNullable = true))
            )),
            Seq()
          )
        ))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET {(  :Person => {name :: STRING IS UNIQUE})}""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeSet(
          graphType(
            nodeType(
              "Person",
              propertyType(
                "name",
                StringType(isNullable = true),
                PropertyInlineUniquenessConstraint()(defaultPos)
              )
            )
          )
        ))
    }
  }

  test("ALTER CURRENT GRAPH TYPE SET {(:Product => {feature :: VECTOR<FLOAT32>(4)})}") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeSet(
          graphType(
            nodeType(
              "Product",
              propertyType(
                "feature",
                VectorType(Some(Float32Type(isNullable = false)(defaultPos)), Some(4), isNullable = true)
              )
            )
          )
        ))
    }
  }

  test("ALTER CURRENT GRAPH TYPE SET {(:Product => {feature :: VECTOR<FLOAT16>(4)})}") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeSet(
          graphType(
            nodeType(
              "Product",
              propertyType(
                "feature",
                VectorType(Some(Float16Type(isNullable = false)(defaultPos)), Some(4), isNullable = true)
              )
            )
          )
        ))
    }
  }

  test("ALTER CURRENT GRAPH TYPE SET {(:Product => {feature :: VECTOR<BFLOAT16>(4)})}") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeSet(
          graphType(
            nodeType(
              "Product",
              propertyType(
                "feature",
                VectorType(Some(BFloat16Type(isNullable = false)(defaultPos)), Some(4), isNullable = true)
              )
            )
          )
        ))
    }
  }

  test(
    "ALTER CURRENT GRAPH TYPE SET {CONSTRAINT independentConstraint FOR (n:Label) REQUIRE n.score IS :: VECTOR(3, INT32)}"
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeSet(
          graphType(
            Seq(),
            Seq(
              propertyTypeConstraint(
                "independentConstraint",
                nodeTypeRefByLabel("Label", "n"),
                ArraySeq(prop("n", "score")),
                VectorType(Some(Integer32Type(isNullable = false)(defaultPos)), Some(3), isNullable = true)(defaultPos)
              )
            )
          )
        ))
    }
  }

  test("ALTER CURRENT GRAPH TYPE SET {(:User)-[:INTERACTS => {score :: VECTOR(1024, FLOAT NOT NULL)}]->()}") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeSet(
          graphType(
            edgeType(
              nodeTypeRefByLabel("User"),
              "INTERACTS",
              EmptyNodeTypeReference()(defaultPos),
              propertyType(
                "score",
                VectorType(Some(FloatType(isNullable = false)(defaultPos)), Some(1024), isNullable = true)
              )
            )
          )
        ))
    }
  }

  test(
    "ALTER CURRENT GRAPH TYPE SET {CONSTRAINT c FOR ()-[r:REL]->() REQUIRE r.feat IS :: VECTOR<INT>(42)}"
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeSet(
          graphType(
            Seq(),
            Seq(
              propertyTypeConstraint(
                "c",
                edgeTypeRefByLabel("REL", "r"),
                ArraySeq(prop("r", "feat")),
                VectorType(Some(IntegerType(isNullable = false)(defaultPos)), Some(42), isNullable = true)(defaultPos)
              )
            )
          )
        ))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET { (:Person)-[r: RELATED IMPLIES{}]->() }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(
          alterCurrentGraphTypeSet(graphType(edgeType(
            nodeTypeRefByLabel("Person"),
            "RELATED",
            "r",
            EmptyNodeTypeReference()(defaultPos)
          )))
        )
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET { ()-[r :RELATED => {reason :: STRING}]->(),
      |CONSTRAINT FOR ()-[r]->() REQUIRE r.reason IS KEY}""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeSet(
          graphType(
            Seq(edgeType(
              EmptyNodeTypeReference()(defaultPos),
              "RELATED",
              "r",
              EmptyNodeTypeReference()(defaultPos),
              propertyType("reason", StringType(isNullable = true))
            )),
            Seq(keyConstraint(edgeTypeRefByVar("r"), ArraySeq(prop("r", "reason"))))
          )
        ))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
      |(c :City => {name :: STRING}),
      |(p)-[l:LIVES_IN => {since :: DATE}]->(c),
      |CONSTRAINT KnowsSinceExist FOR ()-[k:KNOWS]->() REQUIRE k.since IS NOT NULL,
      |CONSTRAINT KnowsSinceType FOR ()-[k:KNOWS]->() REQUIRE k.since :: DATE }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeSet(
          graphType(
            Seq(
              nodeType(
                "Person",
                "p",
                propertyType("name", StringType(isNullable = true)),
                propertyType("age", IntegerType(isNullable = true))
              ),
              nodeType(
                "City",
                "c",
                propertyType("name", StringType(isNullable = true))
              ),
              edgeType(
                nodeTypeRefByVar("p"),
                "LIVES_IN",
                "l",
                nodeTypeRefByVar("c"),
                propertyType("since", DateType(isNullable = true))
              )
            ),
            Seq(
              existsConstraint(
                "KnowsSinceExist",
                edgeTypeRefByLabel("KNOWS", "k"),
                ArraySeq(prop("k", "since"))
              ),
              propertyTypeConstraint(
                "KnowsSinceType",
                edgeTypeRefByLabel("KNOWS", "k"),
                ArraySeq(prop("k", "since")),
                DateType(isNullable = true)(defaultPos)
              )
            )
          )
        ))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET {
      |(p :Person => {name :: STRING, age :: INT}),
      |(c :City => {name :: STRING}),
      |(p)-[l:LIVES_IN => {since :: DATE}]->(c),
      |CONSTRAINT PersonKeyName FOR (p) REQUIRE p.name IS KEY
      |}
      |""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(
          alterCurrentGraphTypeSet(
            graphType(
              Seq(
                nodeType(
                  "Person",
                  "p",
                  propertyType("name", StringType(isNullable = true)),
                  propertyType("age", IntegerType(isNullable = true))
                ),
                nodeType("City", "c", propertyType("name", StringType(isNullable = true))),
                edgeType(
                  nodeTypeRefByVar("p"),
                  "LIVES_IN",
                  "l",
                  nodeTypeRefByVar("c"),
                  propertyType("since", DateType(isNullable = true))
                )
              ),
              Seq(
                keyConstraint("PersonKeyName", nodeTypeRefByVar("p"), ArraySeq(prop("p", "name")))
              )
            )
          )
        )
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET {
      |(p :Person => {name :: STRING, age :: INT}),
      |(c :City => {name :: STRING}),
      |(:Person)-[:LIVES_IN => {since :: DATE}]->(:City),
      |CONSTRAINT PersonKeyName FOR (p) REQUIRE p.name IS KEY
      |}
      |""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(
          alterCurrentGraphTypeSet(
            graphType(
              Seq(
                nodeType(
                  "Person",
                  "p",
                  propertyType("name", StringType(isNullable = true)),
                  propertyType("age", IntegerType(isNullable = true))
                ),
                nodeType("City", "c", propertyType("name", StringType(isNullable = true))),
                edgeType(
                  nodeTypeRefByLabel("Person"),
                  "LIVES_IN",
                  nodeTypeRefByLabel("City"),
                  propertyType("since", DateType(isNullable = true))
                )
              ),
              Seq(
                keyConstraint("PersonKeyName", nodeTypeRefByVar("p"), ArraySeq(prop("p", "name")))
              )
            )
          )
        )
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET {
      |(p :Person => {name :: STRING, age :: INT}),
      |(c :City => {name :: STRING}),
      |(:Person)-[r:LIVES_IN => {since :: DATE}]->(:City) REQUIRE r.since IS UNIQUE OPTIONS {indexProvider : 'range-1.0'}
      |}
      |""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(
          alterCurrentGraphTypeSet(
            graphType(
              Seq(
                nodeType(
                  "Person",
                  "p",
                  propertyType("name", StringType(isNullable = true)),
                  propertyType("age", IntegerType(isNullable = true))
                ),
                nodeType("City", "c", propertyType("name", StringType(isNullable = true))),
                EdgeType(
                  nodeTypeRefByLabel("Person"),
                  Some(varFor("r")),
                  RelTypeName("LIVES_IN")(defaultPos),
                  Set(propertyType("since", DateType(isNullable = true))),
                  nodeTypeRefByLabel("City"),
                  Set((
                    UniquenessConstraint(ArraySeq(prop("r", "since")))(defaultPos),
                    OptionsMap(Map("indexProvider" -> literalString("range-1.0")))(pos)
                  ))
                )(defaultPos)
              ),
              Seq()
            )
          )
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET {(p1 :Person => {name :: STRING}) REQUIRE p2.name IS KEY}""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(
          alterCurrentGraphTypeSet(
            graphType(
              nodeTypeWithConstraints(
                "Person",
                "p1",
                Set(KeyConstraint(ArraySeq(prop("p2", "name")))(defaultPos)),
                propertyType("name", StringType(isNullable = true))
              )
            )
          )
        )
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET {
      |  (p1:Person => :Student) REQUIRE p2.name IS KEY,
      |  (p3)-[k1:KNOWS =>]->(c1) REQUIRE  k2.since IS UNIQUE,
      |  CONSTRAINT FOR (c2) REQUIRE c3.name IS UNIQUE,
      |  CONSTRAINT FOR ()-[k3]->() REQUIRE k4.name IS KEY
      |}""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(
          alterCurrentGraphTypeSet(
            graphType(
              Seq(
                nodeTypeWithLabelsAndConstraints(
                  "Person",
                  "p1",
                  Set("Student"),
                  Set((KeyConstraint(ArraySeq(prop("p2", "name")))(defaultPos), NoOptions))
                ),
                edgeTypeWithConstraints(
                  nodeTypeRefByVar("p3"),
                  "KNOWS",
                  "k1",
                  nodeTypeRefByVar("c1"),
                  Set(UniquenessConstraint(ArraySeq(prop("k2", "since")))(defaultPos))
                )
              ),
              Seq(
                uniquenessConstraint(nodeTypeRefByVar("c2"), ArraySeq(prop("c3", "name"))),
                keyConstraint(edgeTypeRefByVar("k3"), ArraySeq(prop("k4", "name")))
              )
            )
          )
        )
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET { (:Node => :Node2),
      |(:Node =>)-[:REL =>]->(:Node =>) }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(
          alterCurrentGraphTypeSet(
            graphType(
              nodeType("Node", Set("Node2")),
              edgeType(identifyingNodeTypeRef("Node"), "REL", identifyingNodeTypeRef("Node"))
            )
          )
        )
    }
  }

  // Options map

  test("""ALTER CURRENT GRAPH TYPE SET {(p :Person => {name :: STRING}) REQUIRE p.name IS KEY OPTIONS {} }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(
          alterCurrentGraphTypeSet(
            graphType(
              NodeType(
                Some(varFor("p")),
                labelName("Person"),
                Set.empty,
                Set(propertyType("name", StringType(isNullable = true))),
                Set((KeyConstraint(ArraySeq(prop("p", "name")))(pos), OptionsMap(Map.empty)(pos)))
              )(pos)
            )
          )
        )
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET {(p :Person => {name :: STRING}) REQUIRE p.name IS KEY OPTIONS { indexConfig: {} } }"""
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(
          alterCurrentGraphTypeSet(
            graphType(
              NodeType(
                Some(varFor("p")),
                labelName("Person"),
                Set.empty,
                Set(propertyType("name", StringType(isNullable = true))),
                Set((KeyConstraint(ArraySeq(prop("p", "name")))(pos), OptionsMap(Map("indexConfig" -> mapOf()))(pos)))
              )(pos)
            )
          )
        )
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET {(p :Person => {name :: STRING}), CONSTRAINT FOR (p) REQUIRE p.name IS KEY OPTIONS {} }"""
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(
          alterCurrentGraphTypeSet(
            graphType(
              Seq(
                nodeType(
                  "Person",
                  "p",
                  propertyType("name", StringType(isNullable = true))
                )
              ),
              Seq(
                GraphTypeConstraintDefinition(
                  None,
                  nodeTypeRefByVar("p"),
                  KeyConstraint(ArraySeq(prop("p", "name")))(pos),
                  OptionsMap(Map.empty)(pos)
                )(pos)
              )
            )
          )
        )
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET {(p :Person => {name :: STRING}), CONSTRAINT FOR (p) REQUIRE p.name IS KEY OPTIONS { indexConfig: {} } }"""
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(
          alterCurrentGraphTypeSet(
            graphType(
              Seq(
                nodeType(
                  "Person",
                  "p",
                  propertyType("name", StringType(isNullable = true))
                )
              ),
              Seq(
                GraphTypeConstraintDefinition(
                  None,
                  nodeTypeRefByVar("p"),
                  KeyConstraint(ArraySeq(prop("p", "name")))(pos),
                  OptionsMap(Map("indexConfig" -> mapOf()))(pos)
                )(pos)
              )
            )
          )
        )
    }
  }

  // parses but invalid

  test("""ALTER CURRENT GRAPH TYPE SET {(p:Person => :Human), CONSTRAINT FOR (p) REQUIRE p.prop IS NOT NULL }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(
          alterCurrentGraphTypeSet(
            graphType(
              Seq(nodeType("Person", "p", Set("Human"))),
              Seq(
                existsConstraint(
                  nodeTypeRefByVar("p"),
                  ArraySeq(prop("p", "prop"))
                )
              )
            )
          )
        )
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET {(:Person => :Human), CONSTRAINT FOR (p:Person) REQUIRE p.prop IS NOT NULL }"""
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(
          alterCurrentGraphTypeSet(
            graphType(
              Seq(nodeType("Person", Set("Human"))),
              Seq(
                existsConstraint(
                  nodeTypeRefByLabel("Person", "p"),
                  ArraySeq(prop("p", "prop"))
                )
              )
            )
          )
        )
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET {(:Person => :Human), CONSTRAINT FOR (p:Person =>) REQUIRE p.prop IS :: INTEGER }"""
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(
          alterCurrentGraphTypeSet(
            graphType(
              Seq(nodeType("Person", Set("Human"))),
              Seq(
                propertyTypeConstraint(
                  identifyingNodeTypeRef("Person", "p"),
                  ArraySeq(prop("p", "prop")),
                  IntegerType(isNullable = true)(pos)
                )
              )
            )
          )
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR (p:Person =>) REQUIRE p.prop IS :: DATE}""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(
          alterCurrentGraphTypeSet(
            graphType(
              Seq(),
              Seq(
                propertyTypeConstraint(
                  identifyingNodeTypeRef("Person", "p"),
                  ArraySeq(prop("p", "prop")),
                  DateType(isNullable = true)(pos)
                )
              )
            )
          )
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { (:Node =>)-[:REL =>]->() }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(
          alterCurrentGraphTypeSet(
            graphType(
              edgeType(identifyingNodeTypeRef("Node"), "REL", EmptyNodeTypeReference()(pos))
            )
          )
        )
    }
  }

  // fail at semantic checking
  test("""ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR (:Label) REQUIRE (x.prop) IS NOT NULL }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(
          alterCurrentGraphTypeSet(
            graphType(
              Seq(),
              Seq(
                existsConstraint(nodeTypeRefByLabel("Label"), ArraySeq(prop("x", "prop")))
              )
            )
          )
        )
    }
  }

  // fail at semantic checking
  test("""ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR ()-[:REL]->() REQUIRE (x.prop) IS NOT NULL }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(
          alterCurrentGraphTypeSet(
            graphType(
              Seq(),
              Seq(
                existsConstraint(edgeTypeRefByLabel("REL"), ArraySeq(prop("x", "prop")))
              )
            )
          )
        )
    }
  }

  // Generated testcases from examples, see org.neo4j.cypher.internal.graphtype.GraphTypeTestCase
  GraphTypeTestCase.testcases.foreach { case GraphTypeTestCase(name, cypher, ast, _, _) =>
    test(name) {
      Parses(parseAst[Statements](cypher)).in {
        case Cypher5 => cypher5Error
        case _       => _.toAst(alterCurrentGraphTypeSet(ast))
      }
    }
  }

  // Parameters/dynamic labels/relTypes are not allowed

  test("""ALTER CURRENT GRAPH TYPE SET { (:${label} => :Label2) }""") {
    failsParsing[Statements].in {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input '$'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '$', expected: an identifier."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { (:Label => :${label}) }""") {
    failsParsing[Statements].in {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input '$'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '$', expected: an identifier."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { (:Label => {$prop :: STRING}) }""") {
    failsParsing[Statements].in {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input '$'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '$', expected: an identifier or '}'."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { (:${label})-[:REL =>]->() }""") {
    failsParsing[Statements].in {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input '$'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '$', expected: an identifier."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { ()-[:REL =>]->(:${label}) }""") {
    failsParsing[Statements].in {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input '$'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '$', expected: an identifier."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { ()-[:${label} =>]->() }""") {
    failsParsing[Statements].in {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input '$'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '$', expected: an identifier."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { ()-[:REL => {$prop :: STRING}]->() }""") {
    failsParsing[Statements].in {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input '$'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '$', expected: an identifier or '}'."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { CONSTRAINT $name FOR (n:Label) REQUIRE n.prop IS KEY }""") {
    failsParsing[Statements].in {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input '$'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '$', expected: an identifier or 'FOR'."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { CONSTRAINT name FOR (n:${label}) REQUIRE n.prop IS UNIQUE }""") {
    failsParsing[Statements].in {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input '$'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '$', expected: an identifier."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR (n:Label) REQUIRE n.$prop IS NOT NULL }""") {
    failsParsing[Statements].in {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input '$'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '$', expected: an identifier."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { CONSTRAINT $name FOR ()-[n:REL]->() REQUIRE n.prop IS KEY }""") {
    failsParsing[Statements].in {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input '$'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '$', expected: an identifier or 'FOR'."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { CONSTRAINT name FOR ()-[n:${label}]->() REQUIRE n.prop IS :: DATE }""") {
    failsParsing[Statements].in {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input '$'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '$', expected: an identifier."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR ()-[n:Label]->() REQUIRE n.$prop IS NOT NULL }""") {
    failsParsing[Statements].in {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input '$'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '$', expected: an identifier."
        )
    }
  }

  // Negative tests

  test("""ALTER CURRENT GRAPH TYPE SET { CONSTRAINT PersonKeyName }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '}', expected: 'FOR'."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { (:Node => {name :: STRING, name :: STRING}) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "duplicate property key `name`",
          GqlStatusInfoCodes.STATUS_22NC1,
          "error: data exception - graph type element contains duplicated tokens. The graph type element includes a property key with name `name` more than once."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { ()-[:REL => {name :: STRING, name :: STRING}]->() }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "duplicate property key `name`",
          GqlStatusInfoCodes.STATUS_22NC1,
          "error: data exception - graph type element contains duplicated tokens. The graph type element includes a property key with name `name` more than once."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { ()-[:REL => {p :: STRING}]-() }""") {
    // missing direction
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '(', expected: '>'."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { (:Label)<-[:REL =>]-() }""") {
    // wrong direction
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '<', expected: '-'."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { (:Label =>)<-[:REL =>]->(:Label) }""") {
    // double direction
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '<', expected: ',', '-', 'REQUIRE' or '}'."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR ()-[r:REL =>]-() REQUIRE r.p IS KEY }""") {
    // missing direction
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '(', expected: '>'."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { CONSTRAINT name FOR ()<-[r:REL]-() REQUIRE r.p IS NOT NULL }""") {
    // wrong direction
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '<', expected: '-' or 'REQUIRE'."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR ()<-[r:REL]->() REQUIRE r.p IS :: INT }""") {
    // double direction
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '<', expected: '-' or 'REQUIRE'."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { (:Person => {name :: STRING IS REL KEY}) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "node element type property does not allow 'IS RELATIONSHIP KEY'",
          GqlStatusInfoCodes.STATUS_22N04,
          "error: data exception - invalid input value. Invalid input 'IS RELATIONSHIP KEY' for node element type property. Expected 'IS NODE KEY'."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { ()-[:PERSON => {name :: STRING IS NODE KEY}]->() }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "edge element type property does not allow 'IS NODE KEY'",
          GqlStatusInfoCodes.STATUS_22N04,
          "error: data exception - invalid input value. Invalid input 'IS NODE KEY' for edge element type property. Expected 'IS RELATIONSHIP KEY'."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { ()-[p:PERSON => {name :: STRING}]->() REQUIRE p.name IS NODE UNIQUE }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "edge element type does not allow 'IS NODE UNIQUE'",
          GqlStatusInfoCodes.STATUS_22N04,
          "error: data exception - invalid input value. Invalid input 'IS NODE UNIQUE' for edge element type. Expected 'IS RELATIONSHIP UNIQUE'."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { (p:Person => { name :: STRING} ) REQUIRE p.name IS REL UNIQUE }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "node element type does not allow 'IS RELATIONSHIP UNIQUE'",
          GqlStatusInfoCodes.STATUS_22N04,
          "error: data exception - invalid input value. Invalid input 'IS RELATIONSHIP UNIQUE' for node element type. Expected 'IS NODE UNIQUE'."
        )
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET {
      | CONSTRAINT FOR (n:Node) REQUIRE (n.prop) IS RELATIONSHIP KEY
      | }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "'IS RELATIONSHIP KEY' does not allow node patterns",
          GqlStatusInfoCodes.STATUS_22N04,
          "error: data exception - invalid input value. Invalid input 'node pattern' for IS RELATIONSHIP KEY. Expected 'relationship patterns'."
        )
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE SET {
      | CONSTRAINT FOR ()-[r:REL]->() REQUIRE (r.prop) IS NODE UNIQUE
      | }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "'IS NODE UNIQUE' does not allow relationship patterns",
          GqlStatusInfoCodes.STATUS_22N04,
          "error: data exception - invalid input value. Invalid input 'relationship pattern' for IS NODE UNIQUE. Expected 'node patterns'."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { (p:Person => {name :: STRING}) REQUIRE p.name IS NOT NULL }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ =>
        _.withMessageContaining("Property existence constraints cannot be specified inline of a node element type")
    }
  }

  test("""ALTER CURRENT GRAPH TYPE SET { ()-[r:REL => {name :: STRING}]->() REQUIRE r.name IS :: INTEGER }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ =>
        _.withMessageContaining("Property type constraints cannot be specified inline of a relationship element type")
    }
  }

  test("ALTER CURRENT GRAPH TYPE SET { (:L => {p :: STRING IS KEY OPTIONS {}}) }") {
    failsParsing[Statements].in {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'OPTIONS': expected ',' or '}'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input 'OPTIONS', expected: ',' or '}'."
        )
    }
  }

  test("ALTER CURRENT GRAPH TYPE SET { (:L => {p :: STRING IS UNIQUE OPTIONS {}}) }") {
    failsParsing[Statements].in {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'OPTIONS': expected ',' or '}'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input 'OPTIONS', expected: ',' or '}'."
        )
    }
  }

  test("ALTER CURRENT GRAPH TYPE SET { ()-[:R => {p :: STRING IS KEY OPTIONS {}}]->() }") {
    failsParsing[Statements].in {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'OPTIONS': expected ',' or '}'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input 'OPTIONS', expected: ',' or '}'."
        )
    }
  }

  test("ALTER CURRENT GRAPH TYPE SET { ()-[:R => {p :: STRING IS UNIQUE OPTIONS {}}]->() }") {
    failsParsing[Statements].in {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'OPTIONS': expected ',' or '}'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input 'OPTIONS', expected: ',' or '}'."
        )
    }
  }

  // SET generated examples

  // negative example SNT-NE-NEIL-1-1
  test("ALTER CURRENT GRAPH TYPE SET { (:Person {name :: STRING}) }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '{', expected: ')', '=' or 'IMPLIES'."
        )
    }
  }

  // negative example SNT-NE-NEIL-2-1
  test("ALTER CURRENT GRAPH TYPE SET { ({name :: STRING, age :: INT}) }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '{', expected: a variable name, ')' or ':'."
        )
    }
  }

  // negative example SET-NE-NEIL-3-1
  test("ALTER CURRENT GRAPH TYPE SET { (:Person)-[:LIVES_IN {since :: DATE}]->(:City) }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '{', expected: '=' or 'IMPLIES'."
        )
    }
  }

  // negative example SET-NE-NEOL-1-1
  test("ALTER CURRENT GRAPH TYPE SET { ()-[{since :: DATE}]->() }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '{', expected: ':'."
        )
    }
  }

  // negative example SET-NE-NEOL-2-1
  test("""ALTER CURRENT GRAPH TYPE SET { (p :Person => {name STRING, age INT}),
         | (c :City => {name STRING, population INT}),
         | (p)-[{since :: DATE}]->(c) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '{', expected: ':'."
        )
    }
  }

  // negative example SET-NE-NEOL-3-1"
  test(s"""ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
          | (c :City => {name :: STRING, population :: INT}),
          | (p)-[:LIVES_IN&DATED => {since :: DATE}]->(c) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '&', expected: '=' or 'IMPLIES'."
        )
    }
  }

  // negative example SET-NE-NEOL-3-2
  test("""ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
         | (c :City => {name :: STRING, population :: INT}),
         | (p)-[:LIVES_IN => :DATED {since :: DATE}]->(c) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input ':', expected: ']' or '{'."
        )
    }
  }

  // negative example SET-NE-AEML-1-1
  test("""ALTER CURRENT GRAPH TYPE SET { (:Person => {name :: STRING, age :: INT}),
         | (:City => {name :: STRING, population :: INT}),
         | (:Person)-[:LIVES_IN =>]->(:City {name :: STRING}) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '{', expected: ')', '=' or 'IMPLIES'."
        )
    }
  }

  // negative example SET-NE-AEML-2-1
  test(
    "ALTER CURRENT GRAPH TYPE SET { (:Person => {name :: STRING, age :: INT}), (:Person)-[:LIVES_IN =>]->(:City&Named) }"
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '&', expected: ')', '=' or 'IMPLIES'."
        )
    }
  }

  // negative example UDC-NE-CWOS-1-1
  test("ALTER CURRENT GRAPH TYPE SET { CONSTRAINT myConstraint }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '}', expected: 'FOR'."
        )
    }
  }

  // negative example UDC-NE-CWOS-2-1
  test("ALTER CURRENT GRAPH TYPE SET { (:City => {name :: STRING}), CONSTRAINT myConstraint, CONSTRAINT xyz123 }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input ',', expected: 'FOR'."
        )
    }
  }

  // ADD generated examples

  // negative example ADD-NE-NEIL-1-1
  test("""ALTER CURRENT GRAPH TYPE ADD { (:City {name :: STRING}) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '{', expected: ')', '=' or 'IMPLIES'."
        )
    }
  }

  // negative example ADD-NE-NEIL-2-1
  test("""ALTER CURRENT GRAPH TYPE ADD { (:Person => )-[:LIVES_IN {since :: DATE}]->()}""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '{', expected: '=' or 'IMPLIES'."
        )
    }
  }

  // negative example ADD-NE-MIL-1-1
  test("""ALTER CURRENT GRAPH TYPE ADD { (:City&City => :Named)}""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '&', expected: ')', '=' or 'IMPLIES'."
        )
    }
  }

  // negative example ADD-NE-MIL-2-1
  test("""ALTER CURRENT GRAPH TYPE ADD { (:City&Town => {name :: STRING})}""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '&', expected: ')', '=' or 'IMPLIES'."
        )
    }
  }

  // negative example ADD-NE-MIL-3-1
  test("""ALTER CURRENT GRAPH TYPE ADD { (:City&Town&Habitat => :Named {name :: STRING}) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '&', expected: ')', '=' or 'IMPLIES'."
        )
    }
  }

  // negative example ADD-NE-AEML-1-1
  test("""ALTER CURRENT GRAPH TYPE ADD { (:Person => )-[:LIVES_IN => {since :: DATE}]->(:City&Habitat) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '&', expected: ')', '=' or 'IMPLIES'."
        )
    }
  }

  // negative example ADD-NE-AEML-2-1
  test("""ALTER CURRENT GRAPH TYPE ADD { (:Person&Student)-[:LIVES_IN => {since :: DATE}]->(:City =>) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '&', expected: ')', '=' or 'IMPLIES'."
        )
    }
  }

  // negative example ADD-NE-ETNOL-1-1
  test("""ALTER CURRENT GRAPH TYPE ADD { (:Person => )-[{since :: DATE}]->() }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '{', expected: ':'."
        )
    }
  }

  // negative example ADD-NE-ETNOL-2-1
  test("""ALTER CURRENT GRAPH TYPE ADD { (:Person => )-[:LIVES_IN&WORKS_IN {since :: DATE}]->()}""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '&', expected: '=' or 'IMPLIES'."
        )
    }
  }

  // negative example ADD-NE-CWOS-1-1
  test(s"""ALTER CURRENT GRAPH TYPE ADD {  CONSTRAINT myConstraint }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '}', expected: 'FOR'."
        )
    }
  }

  // negative example ADD-NE-CWOS-2-1
  test("""ALTER CURRENT GRAPH TYPE ADD { (:City => {name :: STRING}), CONSTRAINT myConstraint, CONSTRAINT xyz123 }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input ',', expected: 'FOR'."
        )
    }
  }

  // DROP generated examples

  // DROP-NE-UNTCBS-5-1
  test("ALTER CURRENT GRAPH TYPE DROP { (:Person =>), CONSTRAINT FOR (p:Person =>) REQUIRE p.name IS KEY }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '(', expected: ',' or '}'."
        )
    }
  }

  // DROP-NE-UNTCBS-6-1
  test("ALTER CURRENT GRAPH TYPE DROP { (:Person =>), CONSTRAINT FOR (p:Person =>) REQUIRE p.name IS UNIQUE }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '(', expected: ',' or '}'."
        )
    }
  }

  //  DROP-NE-INTCBS-1-1
  test("ALTER CURRENT GRAPH TYPE DROP { CONSTRAINT FOR (s:Student) REQUIRE s.studentID IS NOT NULL }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '(', expected: ',' or '}'."
        )
    }
  }

  // DROP-NE-INTCBS-2-1
  test("ALTER CURRENT GRAPH TYPE DROP { CONSTRAINT FOR (s:Student) REQUIRE s.studentID :: INT }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '(', expected: ',' or '}'."
        )
    }
  }

  // DROP-NE-INTCBS-3-1
  test("ALTER CURRENT GRAPH TYPE DROP { (:Person =>), CONSTRAINT FOR (s:Student) REQUIRE s.studentID IS NOT NULL }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '(', expected: ',' or '}'."
        )
    }
  }

  // DROP-NE-INTCBS-4-1
  test("ALTER CURRENT GRAPH TYPE DROP { (:Person =>), CONSTRAINT FOR (s:Student) REQUIRE s.studentID :: INT }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '(', expected: ',' or '}'."
        )
    }
  }

  // DROP-NE-UETCBS-5-1
  test(
    "ALTER CURRENT GRAPH TYPE DROP { ()-[l:LIVES_IN =>]->(), CONSTRAINT FOR ()-[l:LIVES_IN =>]->() REQUIRE l.since IS KEY }"
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '(', expected: ',' or '}'."
        )
    }
  }

  // DROP-NE-UETCBS-6-1
  test(
    "ALTER CURRENT GRAPH TYPE DROP { ()-[l:LIVES_IN =>]->(), CONSTRAINT FOR ()-[l:LIVES_IN =>]->() REQUIRE l.since IS UNIQUE }"
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '(', expected: ',' or '}'."
        )
    }
  }

  // DROP-NE-IETCBS-1-1
  test("ALTER CURRENT GRAPH TYPE DROP { CONSTRAINT FOR ()-[l:RELATED]->() REQUIRE l.since IS NOT NULL }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '(', expected: ',' or '}'."
        )
    }
  }

  // DROP-NE-IETCBS-2-1
  test("ALTER CURRENT GRAPH TYPE DROP { CONSTRAINT FOR ()-[l:RELATED]->() REQUIRE l.since :: DATE }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '(', expected: ',' or '}'."
        )
    }
  }

  // DROP-NE-IETCBS-3-1
  test(
    "ALTER CURRENT GRAPH TYPE DROP { ()-[:LIVES_IN =>]->(), CONSTRAINT FOR ()-[l:RELATED]->() REQUIRE l.since IS NOT NULL }"
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '(', expected: ',' or '}'."
        )
    }
  }

  // DROP-NE-IETCBS-4-1
  test(
    "ALTER CURRENT GRAPH TYPE DROP { ()-[:LIVES_IN =>]->(), CONSTRAINT FOR ()-[l:RELATED]->() REQUIRE l.since :: DATE }"
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '(', expected: ',' or '}'."
        )
    }
  }

  // Alter generated examples

  // ALTER-NE-NTCUS-7-1 / ALTER-NE-ETCUS-7-1
  test("ALTER CURRENT GRAPH TYPE ALTER { CONSTRAINT myConstraint }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '}', expected: 'FOR'."
        )
    }
  }

  // ALTER-NE-NEIL-1-1
  test("ALTER CURRENT GRAPH TYPE ALTER { (:Person {firstname :: STRING, lastname :: STRING}) }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '{', expected: ')', '=' or 'IMPLIES'."
        )
    }
  }

  // ALTER-NE-NEIL-2-1
  test("ALTER CURRENT GRAPH TYPE ALTER { (:Person =>)-[:LIVES_IN {since :: DATE}]->() }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '{', expected: '=' or 'IMPLIES'."
        )
    }
  }

  // ALTER-NE-AEML-1-1
  test("ALTER CURRENT GRAPH TYPE ALTER { (:Person =>)-[:LIVES_IN => {since :: DATE}]->(:City&Habitat) }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '&', expected: ')', '=' or 'IMPLIES'."
        )
    }
  }

  // ALTER-NE-AEML-2-1
  test("ALTER CURRENT GRAPH TYPE ALTER { (:Person&Student)-[:LIVES_IN => {since :: DATE}]->(:City =>) }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '&', expected: ')', '=' or 'IMPLIES'."
        )
    }
  }

  // ALTER-NE-ETNOL-1-1
  test("ALTER CURRENT GRAPH TYPE ALTER { (:Person => )-[{since :: DATE}]->() }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '{', expected: ':'."
        )
    }
  }

  // ALTER-NE-ETNOL-2-1
  test("ALTER CURRENT GRAPH TYPE ALTER { (:Person => )-[:LIVES_IN&WORKS_IN {since :: DATE}]->() }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '&', expected: '=' or 'IMPLIES'."
        )
    }
  }

}

object GraphTypeParserTest {

  val cypher5Error: Parses[Statements] => Parses[Statements] = _.withSyntaxErrorContaining(
    "Invalid input ",
    GqlStatusInfoCodes.STATUS_42I06,
    "error: syntax error or access rule violation - invalid input. Invalid input 'GRAPH', expected: 'USER SET PASSWORD FROM'."
  )
}
