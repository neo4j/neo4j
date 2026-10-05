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
import org.neo4j.cypher.internal.ast.EmptyNodeTypeReference
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.KeyConstraint
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.UniquenessConstraint
import org.neo4j.cypher.internal.ast.PropertyType.PropertyInlineKeyConstraint
import org.neo4j.cypher.internal.ast.PropertyType.PropertyInlineUniquenessConstraint
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.ddl.GraphTypeParserTest.cypher5Error
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.util.symbols.BFloat16Type
import org.neo4j.cypher.internal.util.symbols.DateType
import org.neo4j.cypher.internal.util.symbols.Float16Type
import org.neo4j.cypher.internal.util.symbols.Float32Type
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.Integer32Type
import org.neo4j.cypher.internal.util.symbols.Integer8Type
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.symbols.VectorType
import org.neo4j.gqlstatus.GqlStatusInfoCodes

import scala.collection.immutable.ArraySeq

class AlterCurrentGraphTypeParserTest extends AstParsingTestBase with AstGraphTypeConstructionTestSupport {

  // Set tests can be found in GraphTypeParserTest

  // Add

  test("""ALTER CURRENT GRAPH TYPE ADD {  }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _       => _.toAst(alterCurrentGraphTypeAdd(graphType()))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | CONSTRAINT FOR (x:Person =>) REQUIRE (x.name) IS UNIQUE
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq.empty,
          Seq(uniquenessConstraint(identifyingNodeTypeRef("Person", "x"), ArraySeq(prop("x", "name"))))
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | CONSTRAINT FOR (x:Person =>) REQUIRE (x.name) IS KEY
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq.empty,
          Seq(keyConstraint(identifyingNodeTypeRef("Person", "x"), ArraySeq(prop("x", "name"))))
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ADD { CONSTRAINT FOR (p:Person) REQUIRE (p.name) IS NODE UNIQUE }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq.empty,
          Seq(uniquenessConstraint(nodeTypeRefByLabel("Person", "p"), ArraySeq(prop("p", "name"))))
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | CONSTRAINT FOR (t:UniqueThingy) REQUIRE (t.name) IS NODE KEY
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq.empty,
          Seq(keyConstraint(nodeTypeRefByLabel("UniqueThingy", "t"), ArraySeq(prop("t", "name"))))
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ADD { CONSTRAINT FOR (t:Thingy) REQUIRE t.uri :: STRING }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq.empty,
          Seq(propertyTypeConstraint(
            nodeTypeRefByLabel("Thingy", "t"),
            ArraySeq(prop("t", "uri")),
            StringType(isNullable = true)(pos)
          ))
        )))
    }
  }

  test("ALTER CURRENT GRAPH TYPE ADD { CONSTRAINT FOR (t:Thingy) REQUIRE t.feature :: VECTOR(3, INT64) }".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq.empty,
          Seq(propertyTypeConstraint(
            nodeTypeRefByLabel("Thingy", "t"),
            ArraySeq(prop("t", "feature")),
            VectorType(Some(IntegerType(isNullable = false)(defaultPos)), Some(3), isNullable = true)(pos)
          ))
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ADD { CONSTRAINT FOR (t:Thingy) REQUIRE t.randomProp IS NOT NULL }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq.empty,
          Seq(existsConstraint(nodeTypeRefByLabel("Thingy", "t"), ArraySeq(prop("t", "randomProp"))))
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | CONSTRAINT FOR (c:City =>) REQUIRE (c.name, c.zip) IS UNIQUE
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq.empty,
          Seq(uniquenessConstraint(identifyingNodeTypeRef("City", "c"), ArraySeq(prop("c", "name"), prop("c", "zip"))))
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | CONSTRAINT FOR (p:Person) REQUIRE (p.name) IS KEY,
      | CONSTRAINT FOR (c:City) REQUIRE (c.name, c.zip) IS UNIQUE
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq.empty,
          Seq(
            keyConstraint(nodeTypeRefByLabel("Person", "p"), ArraySeq(prop("p", "name"))),
            uniquenessConstraint(nodeTypeRefByLabel("City", "c"), ArraySeq(prop("c", "name"), prop("c", "zip")))
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | CONSTRAINT FOR (x:Person =>) REQUIRE (x.name) IS KEY,
      | CONSTRAINT FOR (c:City =>) REQUIRE (c.name, c.zip) IS UNIQUE
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq.empty,
          Seq(
            keyConstraint(identifyingNodeTypeRef("Person", "x"), ArraySeq(prop("x", "name"))),
            uniquenessConstraint(identifyingNodeTypeRef("City", "c"), ArraySeq(prop("c", "name"), prop("c", "zip")))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ADD { CONSTRAINT FOR (p:Person) REQUIRE p.name IS NODE KEY }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq.empty,
          Seq(
            keyConstraint(nodeTypeRefByLabel("Person", "p"), ArraySeq(prop("p", "name")))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ADD { CONSTRAINT FOR (t:UniqueThingy) REQUIRE (t.name) IS UNIQUE }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq.empty,
          Seq(
            uniquenessConstraint(nodeTypeRefByLabel("UniqueThingy", "t"), ArraySeq(prop("t", "name")))
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | CONSTRAINT FOR (t:Thingy) REQUIRE t.randomProp IS NOT NULL,
      | CONSTRAINT FOR (t:Thingy) REQUIRE t.uri :: STRING
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq.empty,
          Seq(
            existsConstraint(nodeTypeRefByLabel("Thingy", "t"), ArraySeq(prop("t", "randomProp"))),
            propertyTypeConstraint(
              nodeTypeRefByLabel("Thingy", "t"),
              ArraySeq(prop("t", "uri")),
              StringType(isNullable = true)(pos)
            )
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | CONSTRAINT notNull FOR (t:Thingy) REQUIRE t.randomProp IS NOT NULL,
      | CONSTRAINT typed FOR (t:Thingy) REQUIRE t.uri :: STRING,
      | CONSTRAINT unique FOR ()-[r:REL]->() REQUIRE (r.prop) IS UNIQUE,
      | CONSTRAINT key FOR ()-[r:REL]->() REQUIRE (r.prop2) IS KEY
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq.empty,
          Seq(
            existsConstraint("notNull", nodeTypeRefByLabel("Thingy", "t"), ArraySeq(prop("t", "randomProp"))),
            propertyTypeConstraint(
              "typed",
              nodeTypeRefByLabel("Thingy", "t"),
              ArraySeq(prop("t", "uri")),
              StringType(isNullable = true)(pos)
            ),
            uniquenessConstraint("unique", edgeTypeRefByLabel("REL", "r"), ArraySeq(prop("r", "prop"))),
            keyConstraint("key", edgeTypeRefByLabel("REL", "r"), ArraySeq(prop("r", "prop2")))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ADD { (p:Person => {name :: STRING IS UNIQUE}) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          nodeTypeWithConstraints(
            "Person",
            "p",
            Set(),
            propertyType("name", StringType(isNullable = true), PropertyInlineUniquenessConstraint()(pos))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ADD { (p:Person => {name :: STRING}) REQUIRE (p.name) IS UNIQUE }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          nodeTypeWithConstraints(
            "Person",
            "p",
            Set(UniquenessConstraint(ArraySeq(prop("p", "name")))(defaultPos)),
            propertyType("name", StringType(isNullable = true))
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | CONSTRAINT FOR (t:Thingy) REQUIRE t.uri :: STRING,
      | CONSTRAINT FOR (t:Thingy) REQUIRE t.randomProp IS NOT NULL,
      | CONSTRAINT FOR (t:UniqueThingy) REQUIRE t.uri :: STRING,
      | CONSTRAINT FOR (t:UniqueThingy) REQUIRE t.uri IS KEY
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq.empty,
          Seq(
            propertyTypeConstraint(
              nodeTypeRefByLabel("Thingy", "t"),
              ArraySeq(prop("t", "uri")),
              StringType(isNullable = true)(pos)
            ),
            existsConstraint(nodeTypeRefByLabel("Thingy", "t"), ArraySeq(prop("t", "randomProp"))),
            propertyTypeConstraint(
              nodeTypeRefByLabel("UniqueThingy", "t"),
              ArraySeq(prop("t", "uri")),
              StringType(isNullable = true)(pos)
            ),
            keyConstraint(nodeTypeRefByLabel("UniqueThingy", "t"), ArraySeq(prop("t", "uri")))
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | (c:City => {name :: STRING}),
      | (:Person =>)-[:LIVES_IN => {since :: DATE}]->(c),
      | (:Person =>)-[:LIKES => {score :: VECTOR<INT32>(1234)}]->(c)
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          nodeType("City", "c", propertyType("name", StringType(isNullable = true))),
          edgeType(
            identifyingNodeTypeRef("Person"),
            "LIVES_IN",
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
          ),
          edgeType(
            identifyingNodeTypeRef("Person"),
            "LIKES",
            nodeTypeRefByVar("c"),
            propertyType(
              "score",
              VectorType(Some(Integer32Type(isNullable = false)(defaultPos)), Some(1234), isNullable = true)
            )
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD { (p:Person => {name :: STRING}), CONSTRAINT FOR (p) REQUIRE (p.name) IS KEY }"""
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq(nodeType("Person", "p", propertyType("name", StringType(isNullable = true)))),
          Seq(keyConstraint(nodeTypeRefByVar("p"), ArraySeq(prop("p", "name"))))
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD { (p:Person => {name :: STRING}), CONSTRAINT FOR (p:Person =>) REQUIRE (p.name) IS UNIQUE}"""
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq(nodeType("Person", "p", propertyType("name", StringType(isNullable = true)))),
          Seq(uniquenessConstraint(identifyingNodeTypeRef("Person", "p"), ArraySeq(prop("p", "name"))))
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | (p:Person => {name :: STRING}),
      | CONSTRAINT FOR (p:Person) REQUIRE (p.name) IS KEY
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq(nodeType("Person", "p", propertyType("name", StringType(isNullable = true)))),
          Seq(keyConstraint(nodeTypeRefByLabel("Person", "p"), ArraySeq(prop("p", "name"))))
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD { (p:Person => {name :: STRING}),
      | CONSTRAINT FOR (p) REQUIRE (p.name) IS UNIQUE }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq(nodeType("Person", "p", propertyType("name", StringType(isNullable = true)))),
          Seq(uniquenessConstraint(nodeTypeRefByVar("p"), ArraySeq(prop("p", "name"))))
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ADD { (:Thingy)-[:RELATED_TO =>]->(:Thingy) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          edgeType(
            nodeTypeRefByLabel("Thingy"),
            "RELATED_TO",
            nodeTypeRefByLabel("Thingy")
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | CONSTRAINT FOR (c:City =>) REQUIRE (c.name, c.zip) IS UNIQUE,
      | CONSTRAINT FOR ()-[l:LIVES_IN =>]->() REQUIRE l.since IS UNIQUE
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq(),
          Seq(
            uniquenessConstraint(identifyingNodeTypeRef("City", "c"), ArraySeq(prop("c", "name"), prop("c", "zip"))),
            uniquenessConstraint(identifyingEdgeTypeRef("LIVES_IN", "l"), ArraySeq(prop("l", "since")))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ADD { (:Person => {name :: STRING}) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          nodeType("Person", propertyType("name", StringType(isNullable = true)))
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ADD { (:Person => :Thingy&Mejig) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          nodeType("Person", Set("Thingy", "Mejig"))
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ADD { (p:Person => {name :: STRING IS KEY}) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = true), PropertyInlineKeyConstraint()(pos))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ADD { (p:Person => {name :: STRING}) REQUIRE (p.name) IS KEY }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          nodeTypeWithConstraints(
            "Person",
            "p",
            Set(KeyConstraint(ArraySeq(prop("p", "name")))(defaultPos)),
            propertyType("name", StringType(isNullable = true))
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | (p:Person => {name :: STRING}),
      | CONSTRAINT FOR (p:Person =>) REQUIRE (p.name) IS KEY
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq(nodeType("Person", "p", propertyType("name", StringType(isNullable = true)))),
          Seq(
            keyConstraint(identifyingNodeTypeRef("Person", "p"), ArraySeq(prop("p", "name")))
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | (p:Person => {name :: STRING}),
      | (c:City => {name :: STRING}),
      | (p)-[:LIVES_IN => {since :: DATE}]->(c)
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          nodeType("Person", "p", propertyType("name", StringType(isNullable = true))),
          nodeType("City", "c", propertyType("name", StringType(isNullable = true))),
          edgeType(
            nodeTypeRefByVar("p"),
            "LIVES_IN",
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | (:Person => {name :: STRING}),
      | (:City => {name :: STRING})
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          nodeType("Person", propertyType("name", StringType(isNullable = true))),
          nodeType("City", propertyType("name", StringType(isNullable = true)))
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD { (:Person =>)-[:OWNS =>]->(:Thingy)}"""
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          edgeType(identifyingNodeTypeRef("Person"), "OWNS", nodeTypeRefByLabel("Thingy"))
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | (:Person =>)-[:OWNS =>]->(:Thingy),
      | (:Thingy)-[:RELATED_TO =>]->(:Thingy),
      | (:Thingy)-[:LOCATED_IN =>]->(:City)
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          edgeType(identifyingNodeTypeRef("Person"), "OWNS", nodeTypeRefByLabel("Thingy")),
          edgeType(nodeTypeRefByLabel("Thingy"), "RELATED_TO", nodeTypeRefByLabel("Thingy")),
          edgeType(nodeTypeRefByLabel("Thingy"), "LOCATED_IN", nodeTypeRefByLabel("City"))
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | (p:Person => {name :: STRING IS KEY}),
      | (c:City => {name :: STRING NOT NULL, zip :: STRING}) REQUIRE (c.name, c.zip) IS UNIQUE,
      | (p)-[l:LIVES_IN => {since :: DATE}]->(c),
      | CONSTRAINT FOR ()-[l]->() REQUIRE l.since IS UNIQUE
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq(
            nodeType(
              "Person",
              "p",
              propertyType("name", StringType(isNullable = true), PropertyInlineKeyConstraint()(pos))
            ),
            nodeTypeWithConstraints(
              "City",
              "c",
              Set(UniquenessConstraint(ArraySeq(prop("c", "name"), prop("c", "zip")))(defaultPos)),
              propertyType("name", StringType(isNullable = false)),
              propertyType("zip", StringType(isNullable = true))
            ),
            edgeType(
              nodeTypeRefByVar("p"),
              "LIVES_IN",
              "l",
              nodeTypeRefByVar("c"),
              propertyType("since", DateType(isNullable = true))
            )
          ),
          Seq(uniquenessConstraint(edgeTypeRefByVar("l"), ArraySeq(prop("l", "since"))))
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | CONSTRAINT FOR (c:City =>) REQUIRE (c.name, c.zip) IS UNIQUE,
      | (:Person =>)-[l:LIVES_IN => {since :: DATE}]->(:City =>),
      | CONSTRAINT FOR ()-[l]->() REQUIRE l.since IS UNIQUE
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq(
            edgeType(
              identifyingNodeTypeRef("Person"),
              "LIVES_IN",
              "l",
              identifyingNodeTypeRef("City"),
              propertyType("since", DateType(isNullable = true))
            )
          ),
          Seq(
            uniquenessConstraint(edgeTypeRefByVar("l"), ArraySeq(prop("l", "since"))),
            uniquenessConstraint(identifyingNodeTypeRef("City", "c"), ArraySeq(prop("c", "name"), prop("c", "zip")))
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD { (:Person =>)-[:LIVES_IN => {since :: DATE}]->(:City =>) }"""
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          edgeType(
            identifyingNodeTypeRef("Person"),
            "LIVES_IN",
            identifyingNodeTypeRef("City"),
            propertyType("since", DateType(isNullable = true))
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | (:City => {name :: STRING})
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          nodeType("City", propertyType("name", StringType(isNullable = true)))
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | (p:Person => {name :: STRING IS KEY}),
      | (c:City => {name :: STRING NOT NULL, zip :: STRING}) REQUIRE (c.name, c.zip) IS UNIQUE
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = true), PropertyInlineKeyConstraint()(pos))
          ),
          nodeTypeWithConstraints(
            "City",
            "c",
            Set(UniquenessConstraint(ArraySeq(prop("c", "name"), prop("c", "zip")))(defaultPos)),
            propertyType("name", StringType(isNullable = false)),
            propertyType("zip", StringType(isNullable = true))
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | (p:Person => {name :: STRING}),
      | CONSTRAINT FOR (p:Person) REQUIRE (p.name) IS UNIQUE
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq(
            nodeType(
              "Person",
              "p",
              propertyType("name", StringType(isNullable = true))
            )
          ),
          Seq(
            uniquenessConstraint(nodeTypeRefByLabel("Person", "p"), ArraySeq(prop("p", "name")))
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | ()-[r:REL => {name :: STRING} ]->(),
      | CONSTRAINT FOR ()-[r:REL]->() REQUIRE (r.name) IS UNIQUE,
      | CONSTRAINT FOR ()-[r:REL]->() REQUIRE (r.id, r.type) IS KEY,
      | CONSTRAINT FOR ()-[q:Quasimodo]->() REQUIRE (q.name) IS NOT NULL,
      | CONSTRAINT FOR ()-[q:Quasimodo]->() REQUIRE (q.instrument) IS :: STRING
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq(
            edgeType(
              EmptyNodeTypeReference()(pos),
              "REL",
              "r",
              EmptyNodeTypeReference()(pos),
              propertyType("name", StringType(isNullable = true))
            )
          ),
          Seq(
            uniquenessConstraint(edgeTypeRefByLabel("REL", "r"), ArraySeq(prop("r", "name"))),
            keyConstraint(edgeTypeRefByLabel("REL", "r"), ArraySeq(prop("r", "id"), prop("r", "type"))),
            existsConstraint(edgeTypeRefByLabel("Quasimodo", "q"), ArraySeq(prop("q", "name"))),
            propertyTypeConstraint(
              edgeTypeRefByLabel("Quasimodo", "q"),
              ArraySeq(prop("q", "instrument")),
              StringType(isNullable = true)(pos)
            )
          )
        )))
    }
  }

  // parse but invalid
  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | CONSTRAINT FOR (x:Person =>) REQUIRE (x.name) IS NOT NULL
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq.empty,
          Seq(existsConstraint(identifyingNodeTypeRef("Person", "x"), ArraySeq(prop("x", "name"))))
        )))
    }
  }

  // parse but invalid
  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | CONSTRAINT FOR (x:Person =>) REQUIRE (x.name) IS :: STRING
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq.empty,
          Seq(propertyTypeConstraint(
            identifyingNodeTypeRef("Person", "x"),
            ArraySeq(prop("x", "name")),
            StringType(isNullable = true)(pos)
          ))
        )))
    }
  }

  // parse but invalid
  test(
    """ALTER CURRENT GRAPH TYPE ADD {
      | CONSTRAINT FOR (x) REQUIRE (x.name) IS KEY
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAdd(graphType(
          Seq.empty,
          Seq(keyConstraint(nodeTypeRefByVar("x"), ArraySeq(prop("x", "name"))))
        )))
    }
  }

  test("ALTER CURRENT GRAPH TYPE ADD { (:L => {p :: STRING IS KEY OPTIONS {}}) }") {
    failsParsing[Statements].in {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'OPTIONS': expected ',' or '}'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input 'OPTIONS', expected: ',' or '}'."
        )
    }
  }

  test("ALTER CURRENT GRAPH TYPE ADD { (:L => {p :: STRING IS UNIQUE OPTIONS {}}) }") {
    failsParsing[Statements].in {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'OPTIONS': expected ',' or '}'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input 'OPTIONS', expected: ',' or '}'."
        )
    }
  }

  test("ALTER CURRENT GRAPH TYPE ADD { ()-[:R => {p :: STRING IS KEY OPTIONS {}}]->() }") {
    failsParsing[Statements].in {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'OPTIONS': expected ',' or '}'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input 'OPTIONS', expected: ',' or '}'."
        )
    }
  }

  test("ALTER CURRENT GRAPH TYPE ADD { ()-[:R => {p :: STRING IS UNIQUE OPTIONS {}}]->() }") {
    failsParsing[Statements].in {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'OPTIONS': expected ',' or '}'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input 'OPTIONS', expected: ',' or '}'."
        )
    }
  }

  // Drop

  test("""ALTER CURRENT GRAPH TYPE DROP {  }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _       => _.toAst(alterCurrentGraphTypeDrop(graphType()))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE DROP { (c:Car => {make :: STRING}), (:Person =>)-[:DRIVES =>]->(c)}""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          nodeType("Car", "c", propertyType("make", StringType(isNullable = true))),
          edgeType(identifyingNodeTypeRef("Person"), "DRIVES", nodeTypeRefByVar("c"))
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE DROP { (:Person =>)-[:DRIVES =>]->(:Car =>) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          edgeType(identifyingNodeTypeRef("Person"), "DRIVES", identifyingNodeTypeRef("Car"))
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE DROP { (:Person => :Student {name :: STRING, age::INT}) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          nodeType(
            "Person",
            Set("Student"),
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          )
        )))
    }
  }

  test("ALTER CURRENT GRAPH TYPE DROP {(p:Product => {feature :: VECTOR<FLOAT32>(4)})}") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          nodeType(
            "Product",
            "p",
            propertyType(
              "feature",
              VectorType(Some(Float32Type(isNullable = false)(defaultPos)), Some(4), isNullable = true)
            )
          )
        )))
    }
  }

  test("ALTER CURRENT GRAPH TYPE DROP {(p:Product => {feature :: VECTOR<FLOAT16>(4)})}") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          nodeType(
            "Product",
            "p",
            propertyType(
              "feature",
              VectorType(Some(Float16Type(isNullable = false)(defaultPos)), Some(4), isNullable = true)
            )
          )
        )))
    }
  }

  test("ALTER CURRENT GRAPH TYPE DROP {(p:Product => {feature :: VECTOR<BFLOAT16>(4)})}") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          nodeType(
            "Product",
            "p",
            propertyType(
              "feature",
              VectorType(Some(BFloat16Type(isNullable = false)(defaultPos)), Some(4), isNullable = true)
            )
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE DROP { CONSTRAINT myConstraint }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          Seq(),
          Seq(
            constraintName("myConstraint")
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE DROP { (:Person => :Student) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          nodeType(
            "Person",
            Set("Student")
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE DROP {
      | (:City =>),
      | (:Person =>)-[:LIVES_IN => {since :: DATE}]->(:City =>),
      | CONSTRAINT myConstraint
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          Seq(
            nodeType("City"),
            edgeType(
              identifyingNodeTypeRef("Person"),
              "LIVES_IN",
              identifyingNodeTypeRef("City"),
              propertyType("since", DateType(isNullable = true))
            )
          ),
          Seq(
            constraintName("myConstraint")
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE DROP { (:Thingy)-[:RELATED_TO =>]->(:Thingy) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          edgeType(
            nodeTypeRefByLabel("Thingy"),
            "RELATED_TO",
            nodeTypeRefByLabel("Thingy")
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE DROP {( :Person => {name :: STRING}), CONSTRAINT myConstraint }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          Seq(
            nodeType("Person", propertyType("name", StringType(isNullable = true)))
          ),
          Seq(
            constraintName("myConstraint")
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE DROP { CONSTRAINT myConstraint, ( :Person => {name :: STRING}) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          Seq(
            nodeType("Person", propertyType("name", StringType(isNullable = true)))
          ),
          Seq(
            constraintName("myConstraint")
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE DROP { (:Person =>) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          nodeType("Person")
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE DROP {
      | (p:Person => {name :: STRING}),
      | (p)-[:DRIVES =>]->(:Car =>)}""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          nodeType("Person", "p", propertyType("name", StringType(isNullable = true))),
          edgeType(
            nodeTypeRefByVar("p"),
            "DRIVES",
            identifyingNodeTypeRef("Car")
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE DROP {
      | (:Person => {name :: STRING}),
      | ()-[:DRIVES =>]->()
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          nodeType("Person", propertyType("name", StringType(isNullable = true))),
          edgeType(
            EmptyNodeTypeReference()(pos),
            "DRIVES",
            EmptyNodeTypeReference()(pos)
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE DROP {
      | (:Person =>),
      | ()-[:DRIVES =>]->() }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          nodeType("Person"),
          edgeType(
            EmptyNodeTypeReference()(pos),
            "DRIVES",
            EmptyNodeTypeReference()(pos)
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE DROP { ()-[:RELATED_TO =>]->() }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          edgeType(
            EmptyNodeTypeReference()(pos),
            "RELATED_TO",
            EmptyNodeTypeReference()(pos)
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE DROP { (:Thingy)-[:RELATED_TO =>]->() }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          edgeType(
            nodeTypeRefByLabel("Thingy"),
            "RELATED_TO",
            EmptyNodeTypeReference()(pos)
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE DROP { ()-[:RELATED_TO =>]->(:Thingy) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          edgeType(
            EmptyNodeTypeReference()(pos),
            "RELATED_TO",
            nodeTypeRefByLabel("Thingy")
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE DROP { (:Thingy)-[:RELATED_TO => {cause::STRING}]->(:Thingy) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          edgeType(
            nodeTypeRefByLabel("Thingy"),
            "RELATED_TO",
            nodeTypeRefByLabel("Thingy"),
            propertyType("cause", StringType(isNullable = true))
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE DROP {
      | CONSTRAINT myConstraint,
      | ()-[:LIVES_IN =>]->()
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          Seq(
            edgeType(EmptyNodeTypeReference()(pos), "LIVES_IN", EmptyNodeTypeReference()(pos))
          ),
          Seq(
            constraintName("myConstraint")
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE DROP {
      | ()-[:LIVES_IN =>]->(), ()-[:KNOWS =>]->(),
      | CONSTRAINT myConstraintB, CONSTRAINT myConstraintA,
      | (p:Person => {name :: STRING}) }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          Seq(
            nodeType("Person", "p", propertyType("name", StringType(isNullable = true))),
            edgeType(
              EmptyNodeTypeReference()(pos),
              "LIVES_IN",
              EmptyNodeTypeReference()(pos)
            ),
            edgeType(
              EmptyNodeTypeReference()(pos),
              "KNOWS",
              EmptyNodeTypeReference()(pos)
            )
          ),
          Seq(
            constraintName("myConstraintB"),
            constraintName("myConstraintA")
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE DROP { ()-[:LIVES_IN =>]->(), CONSTRAINT myConstraint }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          Seq(
            edgeType(
              EmptyNodeTypeReference()(pos),
              "LIVES_IN",
              EmptyNodeTypeReference()(pos)
            )
          ),
          Seq(
            constraintName("myConstraint")
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE DROP {
      | (p:Person => {name :: STRING}),
      | (c:Car => {make :: STRING}),
      | (p)-[:DRIVES =>]->(c)
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          nodeType("Person", "p", propertyType("name", StringType(isNullable = true))),
          nodeType("Car", "c", propertyType("make", StringType(isNullable = true))),
          edgeType(
            nodeTypeRefByVar("p"),
            "DRIVES",
            nodeTypeRefByVar("c")
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE DROP { (:Person => :Student {name :: STRING}) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          nodeType("Person", Set("Student"), propertyType("name", StringType(isNullable = true)))
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE DROP { (:Person => {name :: STRING} ) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          nodeType("Person", propertyType("name", StringType(isNullable = true)))
        )))
    }
  }

  // invalid but parses
  test("""ALTER CURRENT GRAPH TYPE DROP { (a:A => {key::STRING}) REQUIRE a.key IS KEY }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          nodeTypeWithConstraints(
            "A",
            "a",
            Set(KeyConstraint(ArraySeq(prop("a", "key")))(pos)),
            propertyType("key", StringType(isNullable = true))
          )
        )))
    }
  }

  // invalid but parses
  test("""ALTER CURRENT GRAPH TYPE DROP { ()-[a:A => {key::STRING IS UNIQUE}]->() }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeDrop(graphType(
          edgeType(
            EmptyNodeTypeReference()(pos),
            "A",
            "a",
            EmptyNodeTypeReference()(pos),
            propertyType("key", StringType(isNullable = true), PropertyInlineUniquenessConstraint()(pos))
          )
        )))
    }
  }

  // negative tests
  test("ALTER CURRENT GRAPH TYPE DROP { (n) }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '}', expected: '-'."
        )
    }
  }

  test("ALTER CURRENT GRAPH TYPE DROP { ()-[r]->() }") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input ']', expected: ':'."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE DROP { CONSTRAINT FOR (a:A) REQUIRE  a.key IS KEY }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '(', expected: ',' or '}'."
        )
    }
  }

  test("""ALTER CURRENT GRAPH TYPE DROP { CONSTRAINT named FOR (a:A) REQUIRE  a.key IS KEY }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.withSyntaxErrorContaining(
          "Invalid input ",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input 'FOR', expected: ',' or '}'."
        )
    }
  }

  // Alter

  test("""ALTER CURRENT GRAPH TYPE ALTER {  }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _       => _.toAst(alterCurrentGraphTypeAlter(graphType()))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ALTER { (:Person => :L01&L02&L03&L04&L05&L06&L07&L08&L09&L10&L11&L12&L13 {name :: STRING}) }"""
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          nodeType(
            "Person",
            Set("L01", "L02", "L03", "L04", "L05", "L06", "L07", "L08", "L09", "L10", "L11", "L12", "L13"),
            propertyType("name", StringType(isNullable = true))
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ALTER { (:Person =>)-[:LIVES_IN => {since :: DATE NOT NULL, taxClass :: INT, compatibility :: VECTOR<INT8>(42)}]->(:City) }"""
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          edgeType(
            identifyingNodeTypeRef("Person"),
            "LIVES_IN",
            nodeTypeRefByLabel("City"),
            propertyType("since", DateType(isNullable = false)),
            propertyType("taxClass", IntegerType(isNullable = true)),
            propertyType(
              "compatibility",
              VectorType(Some(Integer8Type(isNullable = false)(defaultPos)), Some(42), isNullable = true)
            )
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ALTER {
      | (:Person => :Living {name :: STRING, age :: INT}),
      | (:Animal => :Living {name :: STRING}),
      | (:City => :Location {zip :: STRING NOT NULL, population :: INT}),
      | (:Living)-[:LIVES_IN =>]->(:Location)
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          nodeType(
            "Person",
            Set("Living"),
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          ),
          nodeType("Animal", Set("Living"), propertyType("name", StringType(isNullable = true))),
          nodeType(
            "City",
            Set("Location"),
            propertyType("zip", StringType(isNullable = false)),
            propertyType("population", IntegerType(isNullable = true))
          ),
          edgeType(
            nodeTypeRefByLabel("Living"),
            "LIVES_IN",
            nodeTypeRefByLabel("Location")
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ALTER { (:Person => :Employee {firstname :: STRING, lastname :: STRING, age :: FLOAT, salary :: INT, similarity :: VECTOR<FLOAT>(4095)}) }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          nodeType(
            "Person",
            Set("Employee"),
            propertyType("firstname", StringType(isNullable = true)),
            propertyType("lastname", StringType(isNullable = true)),
            propertyType("age", FloatType(isNullable = true)),
            propertyType("salary", IntegerType(isNullable = true)),
            propertyType(
              "similarity",
              VectorType(Some(FloatType(isNullable = false)(defaultPos)), Some(4095), isNullable = true)
            )
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ALTER { (:Person =>)-[:LIVES_IN => {since :: DATE, taxClass :: INT, rating :: INT}]->(:City =>) }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          edgeType(
            identifyingNodeTypeRef("Person"),
            "LIVES_IN",
            identifyingNodeTypeRef("City"),
            propertyType("since", DateType(isNullable = true)),
            propertyType("taxClass", IntegerType(isNullable = true)),
            propertyType("rating", IntegerType(isNullable = true))
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ALTER {
      | (p:Person => {name :: STRING}),
      | (c:Car => :Vehicle {make :: STRING NOT NULL}),
      | (p)-[:DRIVES =>]->(c)
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          nodeType("Person", "p", propertyType("name", StringType(isNullable = true))),
          nodeType("Car", "c", Set("Vehicle"), propertyType("make", StringType(isNullable = false))),
          edgeType(
            nodeTypeRefByVar("p"),
            "DRIVES",
            nodeTypeRefByVar("c")
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ALTER {
      | (:Person => {name :: STRING, age :: INT}),
      | (:Animal =>)-[:LIVES_IN => {observationFrequency :: FLOAT}]->(:Habitat)
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          nodeType(
            "Person",
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          ),
          edgeType(
            identifyingNodeTypeRef("Animal"),
            "LIVES_IN",
            nodeTypeRefByLabel("Habitat"),
            propertyType("observationFrequency", FloatType(isNullable = true))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ALTER { (:Person => {name :: STRING}) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          nodeType("Person", propertyType("name", StringType(isNullable = true)))
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ALTER {
      | (c:Car => :Vehicle {make :: STRING NOT NULL}),
      | (:Person =>)-[:DRIVES =>]->(c)
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          nodeType("Car", "c", Set("Vehicle"), propertyType("make", StringType(isNullable = false))),
          edgeType(
            identifyingNodeTypeRef("Person"),
            "DRIVES",
            nodeTypeRefByVar("c")
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ALTER { (:Person => {name :: STRING NOT NULL, age :: INT}) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          nodeType(
            "Person",
            propertyType("name", StringType(isNullable = false)),
            propertyType("age", IntegerType(isNullable = true))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ALTER { (:Person => {name :: STRING, age :: INT}) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          nodeType(
            "Person",
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ALTER { (:Person => :L07&L12 {name :: STRING}) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          nodeType(
            "Person",
            Set("L07", "L12"),
            propertyType("name", StringType(isNullable = true))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ALTER { ()-[:LIVES_IN => {since :: DATE}]->(:City) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          edgeType(
            EmptyNodeTypeReference()(pos),
            "LIVES_IN",
            nodeTypeRefByLabel("City"),
            propertyType("since", DateType(isNullable = true))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ALTER { (:Animal)-[:LIVES_IN => {observationFrequency :: FLOAT}]->(:Habitat) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          edgeType(
            nodeTypeRefByLabel("Animal"),
            "LIVES_IN",
            nodeTypeRefByLabel("Habitat"),
            propertyType("observationFrequency", FloatType(isNullable = true))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ALTER { (:Person =>)-[:LIVES_IN => {since :: DATE}]->() }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          edgeType(
            identifyingNodeTypeRef("Person"),
            "LIVES_IN",
            EmptyNodeTypeReference()(pos),
            propertyType("since", DateType(isNullable = true))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ALTER { (:Person =>)-[:LIVES_IN => {since :: DATE, taxClass :: INT}]->(:City) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          edgeType(
            identifyingNodeTypeRef("Person"),
            "LIVES_IN",
            nodeTypeRefByLabel("City"),
            propertyType("since", DateType(isNullable = true)),
            propertyType("taxClass", IntegerType(isNullable = true))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ALTER { (:Person => {name :: STRING, ssn :: STRING, age :: INT}) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          nodeType(
            "Person",
            propertyType("name", StringType(isNullable = true)),
            propertyType("ssn", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ALTER { (:Living)-[:LIVES_IN => {since :: DATE}]->(:Location) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          edgeType(
            nodeTypeRefByLabel("Living"),
            "LIVES_IN",
            nodeTypeRefByLabel("Location"),
            propertyType("since", DateType(isNullable = true))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ALTER { (:Person =>)-[:LIVES_IN => {since :: DATE NOT NULL}]->(:City) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          edgeType(
            identifyingNodeTypeRef("Person"),
            "LIVES_IN",
            nodeTypeRefByLabel("City"),
            propertyType("since", DateType(isNullable = false))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ALTER { (:City =>)-[:LIVES_IN => {since :: DATE}]->(:City =>) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          edgeType(
            identifyingNodeTypeRef("City"),
            "LIVES_IN",
            identifyingNodeTypeRef("City"),
            propertyType("since", DateType(isNullable = true))
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ALTER { (:Person => :Student {name :: STRING, ssn :: STRING, age :: INT}) }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          nodeType(
            "Person",
            Set("Student"),
            propertyType("name", StringType(isNullable = true)),
            propertyType("ssn", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ALTER { (:Animal =>)-[:LIVES_IN => {observationFrequency :: FLOAT}]->(:Habitat) }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          edgeType(
            identifyingNodeTypeRef("Animal"),
            "LIVES_IN",
            nodeTypeRefByLabel("Habitat"),
            propertyType("observationFrequency", FloatType(isNullable = true))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ALTER { (:Person => :Employee&Stressed&Adult {name :: STRING}) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          nodeType(
            "Person",
            Set("Employee", "Stressed", "Adult"),
            propertyType("name", StringType(isNullable = true))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ALTER { (:Person =>)-[:LIVES_IN => {since :: DATE}]->(:City) }""".stripMargin) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          edgeType(
            identifyingNodeTypeRef("Person"),
            "LIVES_IN",
            nodeTypeRefByLabel("City"),
            propertyType("since", DateType(isNullable = true))
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ALTER { (:Person => :Employee {name :: STRING, age :: FLOAT, salary :: INT}) }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          nodeType(
            "Person",
            Set("Employee"),
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", FloatType(isNullable = true)),
            propertyType("salary", IntegerType(isNullable = true))
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ALTER { (:Person => :L01&L02&L03&L04&L05&L07&L08&L09&L11&L12&L13 {name :: STRING}) }"""
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          nodeType(
            "Person",
            Set("L01", "L02", "L03", "L04", "L05", "L07", "L08", "L09", "L11", "L12", "L13"),
            propertyType("name", StringType(isNullable = true))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ALTER { (:Living)-[:LIVES_IN => {since :: DATE}]->(:City =>) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          edgeType(
            nodeTypeRefByLabel("Living"),
            "LIVES_IN",
            identifyingNodeTypeRef("City"),
            propertyType("since", DateType(isNullable = true))
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ALTER {
      | (:Person => {name :: STRING, age :: INT}),
      | (:City => :Location {name :: STRING, population :: INT}),
      | (:Person =>)-[:LIVES_IN => {address :: STRING, since :: DATE}]->(:Location)
  }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          nodeType(
            "Person",
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          ),
          nodeType(
            "City",
            Set("Location"),
            propertyType("name", StringType(isNullable = true)),
            propertyType("population", IntegerType(isNullable = true))
          ),
          edgeType(
            identifyingNodeTypeRef("Person"),
            "LIVES_IN",
            nodeTypeRefByLabel("Location"),
            propertyType("address", StringType(isNullable = true)),
            propertyType("since", DateType(isNullable = true))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ALTER { ()-[:LIVES_IN => {since :: DATE}]->() }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          edgeType(
            EmptyNodeTypeReference()(pos),
            "LIVES_IN",
            EmptyNodeTypeReference()(pos),
            propertyType("since", DateType(isNullable = true))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ALTER { (:Person => )-[:LIVES_IN => {since :: DATE}]->(:Geographic) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          edgeType(
            identifyingNodeTypeRef("Person"),
            "LIVES_IN",
            nodeTypeRefByLabel("Geographic"),
            propertyType("since", DateType(isNullable = true))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ALTER { (:Person => :Student {name :: STRING}) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          nodeType(
            "Person",
            Set("Student"),
            propertyType("name", StringType(isNullable = true))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ALTER { (:Person => :Employee {name :: STRING}) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          nodeType(
            "Person",
            Set("Employee"),
            propertyType("name", StringType(isNullable = true))
          )
        )))
    }
  }

  test("""ALTER CURRENT GRAPH TYPE ALTER { (:Person => :Student {name :: STRING, age :: INT}) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          nodeType(
            "Person",
            Set("Student"),
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          )
        )))
    }
  }

  // not valid, but will parse

  test("""ALTER CURRENT GRAPH TYPE ALTER { (:Person => {name :: STRING IS KEY}) }""") {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          nodeType(
            "Person",
            propertyType("name", StringType(isNullable = true), PropertyInlineKeyConstraint()(pos))
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ALTER { ()-[p:PERSON => {name :: STRING}]->() REQUIRE p.name IS RELATIONSHIP UNIQUE }"""
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          edgeTypeWithConstraints(
            EmptyNodeTypeReference()(pos),
            "PERSON",
            "p",
            EmptyNodeTypeReference()(pos),
            Set(UniquenessConstraint(ArraySeq(prop("p", "name")))(pos)),
            propertyType("name", StringType(isNullable = true))
          )
        )))
    }
  }

  test(
    """ALTER CURRENT GRAPH TYPE ALTER {
      | CONSTRAINT FOR (n:Node) REQUIRE (n.prop) IS NODE KEY
      | }""".stripMargin
  ) {
    parsesIn[Statements] {
      case Cypher5 => cypher5Error
      case _ => _.toAst(alterCurrentGraphTypeAlter(graphType(
          Seq(),
          Seq(
            keyConstraint(nodeTypeRefByLabel("Node", "n"), ArraySeq(prop("n", "prop")))
          )
        )))
    }
  }
}
