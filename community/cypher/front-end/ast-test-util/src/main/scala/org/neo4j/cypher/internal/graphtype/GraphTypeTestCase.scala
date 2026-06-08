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
package org.neo4j.cypher.internal.graphtype

import org.neo4j.cypher.internal.ast.AstGraphTypeConstructionTestSupport
import org.neo4j.cypher.internal.ast.EmptyNodeTypeReference
import org.neo4j.cypher.internal.ast.GraphType
import org.neo4j.cypher.internal.ast.GraphTypeConstraint
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.KeyConstraint
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.UniquenessConstraint
import org.neo4j.cypher.internal.ast.GraphTypeEntry
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.PropertyType.PropertyInlineKeyConstraint
import org.neo4j.cypher.internal.ast.PropertyType.PropertyInlineUniquenessConstraint
import org.neo4j.cypher.internal.util.symbols.AnyType
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.DateType
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.StringType

import scala.collection.immutable.ArraySeq

case class GraphTypeTestCase(
  name: String,
  cypher: String,
  ast: GraphType,
  prettifiedCypher: String,
  canonicalPrettifiedCypher: String = "{}"
) {
  override def toString: String = name
}

object GraphTypeTestCase extends AstGraphTypeConstructionTestSupport {

  private def re(): Seq[GraphTypeTestCase] = Seq(GraphTypeTestCase(
    "RE-1-1",
    """ALTER CURRENT GRAPH TYPE SET { (s:Student => :Person { name :: STRING NOT NULL, birthday :: DATE, studId :: INT IS KEY })
      |  REQUIRE (s.name, s.birthday) IS UNIQUE OPTIONS { indexProvider: "range-1.0" },
      |  (:City => :Location { name :: STRING IS KEY }),
      |  (:Site => :Location { name :: STRING }),
      |  (s)-[:LIVES_IN =>]->(:City =>),
      |  (s)-[:VISITED =>]->(:Location),
      |  CONSTRAINT FOR ()-[x:LegacyRel]->() REQUIRE x.foo IS UNIQUE,
      |  CONSTRAINT mySiteConstraint FOR (st:Site =>) REQUIRE st.name IS KEY,
      |  CONSTRAINT FOR (p:Person) REQUIRE p.age :: INT }""".stripMargin,
    graphType(
      Seq[GraphTypeEntry](
        nodeTypeWithLabelsAndConstraints(
          "Student",
          "s",
          Set("Person"),
          Set((
            UniquenessConstraint(ArraySeq(prop(varFor("s"), "name"), prop(varFor("s"), "birthday")))(defaultPos),
            OptionsMap(Map("indexProvider" -> literalString("range-1.0")))(pos)
          )),
          propertyType("name", StringType(isNullable = false)),
          propertyType("studId", IntegerType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos)),
          propertyType("birthday", DateType(isNullable = true))
        ),
        nodeType(
          "City",
          Set("Location"),
          propertyType("name", StringType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos))
        ),
        nodeType("Site", Set("Location"), propertyType("name", StringType(isNullable = true))),
        edgeType(nodeTypeRefByVar("s"), "LIVES_IN", identifyingNodeTypeRef("City")),
        edgeType(nodeTypeRefByVar("s"), "VISITED", nodeTypeRefByLabel("Location"))
      ),
      Seq[GraphTypeConstraint](
        uniquenessConstraint(edgeTypeRefByLabel("LegacyRel", "x"), ArraySeq(prop(varFor("x"), "foo"))),
        keyConstraint(
          "mySiteConstraint",
          identifyingNodeTypeRef("Site", "st"),
          ArraySeq(prop(varFor("st"), "name"))
        ),
        propertyTypeConstraint(
          nodeTypeRefByLabel("Person", "p"),
          ArraySeq(prop(varFor("p"), "age")),
          IntegerType(isNullable = true)(defaultPos)
        )
      )
    ),
    """{
      |  (:`City` => :`Location` {`name` :: STRING IS KEY}),
      |  (:`Site` => :`Location` {`name` :: STRING}),
      |  (`s`:`Student` => :`Person` {`birthday` :: DATE, `name` :: STRING NOT NULL, `studId` :: INTEGER IS KEY}) REQUIRE (`s`.`name`, `s`.`birthday`) IS UNIQUE OPTIONS {`indexProvider`: "range-1.0"},
      |  (`s`)-[:`LIVES_IN` =>]->(:`City` =>),
      |  (`s`)-[:`VISITED` =>]->(:`Location`),
      |  CONSTRAINT `mySiteConstraint` FOR (`st`:`Site` =>) REQUIRE (`st`.`name`) IS KEY,
      |  CONSTRAINT FOR (`p`:`Person`) REQUIRE (`p`.`age`) IS :: INTEGER,
      |  CONSTRAINT FOR ()-[`x`:`LegacyRel`]->() REQUIRE (`x`.`foo`) IS UNIQUE
      |}""".stripMargin,
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
  ))

  private def snt(): Seq[GraphTypeTestCase] = Seq(
    GraphTypeTestCase(
      "SNT-PE-PI-1-1",
      """ALTER CURRENT GRAPH TYPE SET { ( :Person => { name  :: STRING}) }""",
      graphType(nodeType("Person", propertyType("name", StringType(isNullable = true)))),
      """{
        |  (:`Person` => {`name` :: STRING})
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-PI-1-2",
      """ALTER CURRENT GRAPH TYPE SET { (:Person IMPLIES { name  :: STRING}) }""",
      graphType(nodeType("Person", propertyType("name", StringType(isNullable = true)))),
      """{
        |  (:`Person` => {`name` :: STRING})
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-PI-1-3",
      """ALTER CURRENT GRAPH TYPE SET { ( p: Person => { name  :: STRING}) }""",
      graphType(nodeType("Person", "p", propertyType("name", StringType(isNullable = true)))),
      """{
        |  (`p`:`Person` => {`name` :: STRING})
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-PI-1-4",
      """ALTER CURRENT GRAPH TYPE SET { ( p : Person IMPLIES  { name  :: STRING}) }""",
      graphType(nodeType("Person", "p", propertyType("name", StringType(isNullable = true)))),
      """{
        |  (`p`:`Person` => {`name` :: STRING})
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-PI-1-5",
      """ALTER CURRENT GRAPH TYPE SET { (`p`:`Person` =>      {`name` :: STRING}) }""",
      graphType(nodeType("Person", "p", propertyType("name", StringType(isNullable = true)))),
      """{
        |  (`p`:`Person` => {`name` :: STRING})
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-PI-1-6",
      """ALTER CURRENT GRAPH TYPE SET { (`p`:`Person` IMPLIES {`name` :: STRING}) }""",
      graphType(nodeType("Person", "p", propertyType("name", StringType(isNullable = true)))),
      """{
        |  (`p`:`Person` => {`name` :: STRING})
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-PI-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => {name       STRING!,         age       INT}) }""",
      graphType(nodeType(
        "Person",
        propertyType("name", StringType(isNullable = false)),
        propertyType("age", IntegerType(isNullable = true))
      )),
      """{
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL})
        |}""".stripMargin,
      """{
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-PI-2-2",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => {name ::    STRING!,         age ::    INT}) }""",
      graphType(nodeType(
        "Person",
        propertyType("name", StringType(isNullable = false)),
        propertyType("age", IntegerType(isNullable = true))
      )),
      """{
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL})
        |}""".stripMargin,
      """{
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-PI-2-3",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => {name       STRING NOT NULL, age       INT}) }""",
      graphType(nodeType(
        "Person",
        propertyType("name", StringType(isNullable = false)),
        propertyType("age", IntegerType(isNullable = true))
      )),
      """{
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL})
        |}""".stripMargin,
      """{
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-PI-2-4",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => {name ::    STRING NOT NULL, age ::    INT}) }""",
      graphType(nodeType(
        "Person",
        propertyType("name", StringType(isNullable = false)),
        propertyType("age", IntegerType(isNullable = true))
      )),
      """{
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL})
        |}""".stripMargin,
      """{
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-PI-2-5",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => {name TYPED STRING NOT NULL, age TYPED INT}) }""",
      graphType(nodeType(
        "Person",
        propertyType("name", StringType(isNullable = false)),
        propertyType("age", IntegerType(isNullable = true))
      )),
      """{
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL})
        |}""".stripMargin,
      """{
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-LI-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => :Student             ) }""",
      graphType(nodeType("Person", Set("Student"))),
      """{
        |  (:`Person` => :`Student`)
        |}""".stripMargin,
      """{
        |  (:`Person` => :`Student`)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-LI-1-2",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => :Student         {  }) }""",
      graphType(nodeType("Person", Set("Student"))),
      """{
        |  (:`Person` => :`Student`)
        |}""".stripMargin,
      """{
        |  (:`Person` => :`Student`)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-LI-1-3",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => :Student&Student {  }) }""",
      graphType(nodeType("Person", Set("Student"))),
      """{
        |  (:`Person` => :`Student`)
        |}""".stripMargin,
      """{
        |  (:`Person` => :`Student`)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-LI-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => :Student&Happy) }""",
      graphType(nodeType("Person", Set("Student", "Happy"))),
      """{
        |  (:`Person` => :`Happy`&`Student`)
        |}""".stripMargin,
      """{
        |  (:`Person` => :`Happy`&`Student`)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-LI-2-2",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => :Happy&Student) }""",
      graphType(nodeType("Person", Set("Student", "Happy"))),
      """{
        |  (:`Person` => :`Happy`&`Student`)
        |}""".stripMargin,
      """{
        |  (:`Person` => :`Happy`&`Student`)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-LI-2-3",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => :Happy&Student&Happy) }""",
      graphType(nodeType("Person", Set("Student", "Happy"))),
      """{
        |  (:`Person` => :`Happy`&`Student`)
        |}""".stripMargin,
      """{
        |  (:`Person` => :`Happy`&`Student`)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-LI-2-4",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => :Student&Student&Happy) }""",
      graphType(nodeType("Person", Set("Student", "Happy"))),
      """{
        |  (:`Person` => :`Happy`&`Student`)
        |}""".stripMargin,
      """{
        |  (:`Person` => :`Happy`&`Student`)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-LI-2-5",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => :Happy&Student&Student&Happy&Student) }""",
      graphType(nodeType("Person", Set("Student", "Happy"))),
      """{
        |  (:`Person` => :`Happy`&`Student`)
        |}""".stripMargin,
      """{
        |  (:`Person` => :`Happy`&`Student`)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-LPI-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => :Student {name :: STRING}) }""",
      graphType(nodeType("Person", Set("Student"), propertyType("name", StringType(isNullable = true)))),
      """{
        |  (:`Person` => :`Student` {`name` :: STRING})
        |}""".stripMargin,
      """{
        |  (:`Person` => :`Student` {`name` :: STRING})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-LPI-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => :Student&Happy
        |  {name :: STRING NOT NULL, age :: INT, studentID :: STRING}) }""".stripMargin,
      graphType(nodeType(
        "Person",
        Set("Student", "Happy"),
        propertyType("name", StringType(isNullable = false)),
        propertyType("age", IntegerType(isNullable = true)),
        propertyType("studentID", StringType(isNullable = true))
      )),
      """{
        |  (:`Person` => :`Happy`&`Student` {`age` :: INTEGER, `name` :: STRING NOT NULL, `studentID` :: STRING})
        |}""".stripMargin,
      """{
        |  (:`Person` => :`Happy`&`Student` {`age` :: INTEGER, `name` :: STRING NOT NULL, `studentID` :: STRING})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-SPVT-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => {data :: ANY NOT NULL}) }""",
      graphType(nodeType(
        "Person",
        propertyType("data", AnyType(isNullable = false))
      )),
      """{
        |  (:`Person` => {`data` :: ANY NOT NULL})
        |}""".stripMargin,
      """{
        |  (:`Person` => {`data` :: ANY NOT NULL})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-SPVT-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => {name :: STRING NOT NULL, age :: INT|STRING     }) }""".stripMargin,
      graphType(nodeType(
        "Person",
        propertyType("name", StringType(isNullable = false)),
        propertyType(
          "age",
          ClosedDynamicUnionType(Set(
            IntegerType(isNullable = true)(defaultPos),
            StringType(isNullable = true)(defaultPos)
          ))
        )
      )),
      """{
        |  (:`Person` => {`age` :: STRING | INTEGER, `name` :: STRING NOT NULL})
        |}""".stripMargin,
      """{
        |  (:`Person` => {`age` :: STRING | INTEGER, `name` :: STRING NOT NULL})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-SPVT-2-2",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => {name :: STRING NOT NULL, age :: ANY<INT|STRING>}) }""".stripMargin,
      graphType(nodeType(
        "Person",
        propertyType("name", StringType(isNullable = false)),
        propertyType(
          "age",
          ClosedDynamicUnionType(Set(
            IntegerType(isNullable = true)(defaultPos),
            StringType(isNullable = true)(defaultPos)
          ))
        )
      )),
      """{
        |  (:`Person` => {`age` :: STRING | INTEGER, `name` :: STRING NOT NULL})
        |}""".stripMargin,
      """{
        |  (:`Person` => {`age` :: STRING | INTEGER, `name` :: STRING NOT NULL})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-SPVT-2-3",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => {name :: STRING NOT NULL, age :: ANY<STRING|INT>}) }""",
      graphType(nodeType(
        "Person",
        propertyType("name", StringType(isNullable = false)),
        propertyType(
          "age",
          ClosedDynamicUnionType(Set(
            IntegerType(isNullable = true)(defaultPos),
            StringType(isNullable = true)(defaultPos)
          ))
        )
      )),
      """{
        |  (:`Person` => {`age` :: STRING | INTEGER, `name` :: STRING NOT NULL})
        |}""".stripMargin,
      """{
        |  (:`Person` => {`age` :: STRING | INTEGER, `name` :: STRING NOT NULL})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-SPVT-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => {names :: LIST<STRING NOT NULL>}) }""",
      graphType(nodeType(
        "Person",
        propertyType(
          "names",
          ListType(StringType(isNullable = false)(defaultPos), isNullable = true)
        )
      )),
      """{
        |  (:`Person` => {`names` :: LIST<STRING NOT NULL>})
        |}""".stripMargin,
      """{
        |  (:`Person` => {`names` :: LIST<STRING NOT NULL>})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKC-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (  :Person => {name :: STRING IS KEY}) }""",
      graphType(nodeType(
        "Person",
        propertyType(
          "name",
          StringType(isNullable = true),
          PropertyInlineKeyConstraint()(defaultPos)
        )
      )),
      """{
        |  (:`Person` => {`name` :: STRING IS KEY})
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKC-1-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING}) REQUIRE p.name IS KEY }""",
      graphType(
        nodeTypeWithConstraints(
          "Person",
          "p",
          Set(KeyConstraint(ArraySeq(prop("p", "name")))(defaultPos)),
          propertyType(
            "name",
            StringType(isNullable = true)
          )
        )
      ),
      """{
        |  (`p`:`Person` => {`name` :: STRING}) REQUIRE (`p`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKC-1-3",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING}), CONSTRAINT FOR (p) REQUIRE p.name IS KEY }""",
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = true)
          )
        )),
        Seq(
          keyConstraint(nodeTypeRefByVar("p"), ArraySeq(prop("p", "name")))
        )
      ),
      """{
        |  (`p`:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKC-1-4",
      """ALTER CURRENT GRAPH TYPE SET { (  :Person => {name :: STRING}),
        |  CONSTRAINT FOR (p :Person) REQUIRE p.name IS KEY }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          propertyType(
            "name",
            StringType(isNullable = true)
          )
        )),
        Seq(
          keyConstraint(nodeTypeRefByLabel("Person", "p"), ArraySeq(prop("p", "name")))
        )
      ),
      """{
        |  (:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`p`:`Person`) REQUIRE (`p`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKC-1-5",
      """ALTER CURRENT GRAPH TYPE SET { (  :Person => {name :: STRING}),
        |  CONSTRAINT FOR (p :Person =>) REQUIRE p.name IS KEY }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          propertyType(
            "name",
            StringType(isNullable = true)
          )
        )),
        Seq(
          keyConstraint(identifyingNodeTypeRef("Person", "p"), ArraySeq(prop("p", "name")))
        )
      ),
      """{
        |  (:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`p`:`Person` =>) REQUIRE (`p`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKC-1-6",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING}),
        |  CONSTRAINT FOR (bar :Person) REQUIRE bar.name IS KEY }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = true)
          )
        )),
        Seq(
          keyConstraint(nodeTypeRefByLabel("Person", "bar"), ArraySeq(prop("bar", "name")))
        )
      ),
      """{
        |  (`p`:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`bar`:`Person`) REQUIRE (`bar`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKC-1-7",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING}),
        |  CONSTRAINT FOR (bar :Person =>) REQUIRE bar.name IS KEY }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = true)
          )
        )),
        Seq(
          keyConstraint(identifyingNodeTypeRef("Person", "bar"), ArraySeq(prop("bar", "name")))
        )
      ),
      """{
        |  (`p`:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`bar`:`Person` =>) REQUIRE (`bar`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKC-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (  :Person => {name :: STRING NOT NULL IS KEY}) }""",
      graphType(
        nodeType(
          "Person",
          propertyType(
            "name",
            StringType(isNullable = false),
            PropertyInlineKeyConstraint()(defaultPos)
          )
        )
      ),
      """{
        |  (:`Person` => {`name` :: STRING NOT NULL IS KEY})
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKC-2-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING NOT NULL}) REQUIRE p.name IS KEY }""",
      graphType(
        nodeTypeWithConstraints(
          "Person",
          "p",
          Set(KeyConstraint(ArraySeq(prop("p", "name")))(defaultPos)),
          propertyType(
            "name",
            StringType(isNullable = false)
          )
        )
      ),
      """{
        |  (`p`:`Person` => {`name` :: STRING NOT NULL}) REQUIRE (`p`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKC-2-3",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING NOT NULL}), CONSTRAINT FOR (p) REQUIRE p.name IS KEY }""",
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = false)
          )
        )),
        Seq(
          keyConstraint(nodeTypeRefByVar("p"), ArraySeq(prop("p", "name")))
        )
      ),
      """{
        |  (`p`:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKC-2-4",
      """ALTER CURRENT GRAPH TYPE SET { (  :Person => {name :: STRING NOT NULL}),
        |  CONSTRAINT FOR (p :Person) REQUIRE p.name IS KEY }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          propertyType(
            "name",
            StringType(isNullable = false)
          )
        )),
        Seq(
          keyConstraint(nodeTypeRefByLabel("Person", "p"), ArraySeq(prop("p", "name")))
        )
      ),
      """{
        |  (:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`p`:`Person`) REQUIRE (`p`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKC-2-5",
      """ALTER CURRENT GRAPH TYPE SET { (  :Person => {name :: STRING NOT NULL}),
        |  CONSTRAINT FOR (p :Person =>) REQUIRE p.name IS KEY }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          propertyType(
            "name",
            StringType(isNullable = false)
          )
        )),
        Seq(
          keyConstraint(identifyingNodeTypeRef("Person", "p"), ArraySeq(prop("p", "name")))
        )
      ),
      """{
        |  (:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`p`:`Person` =>) REQUIRE (`p`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKC-2-6",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING NOT NULL}),
        |  CONSTRAINT FOR (bar :Person) REQUIRE bar.name IS KEY }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = false)
          )
        )),
        Seq(
          keyConstraint(nodeTypeRefByLabel("Person", "bar"), ArraySeq(prop("bar", "name")))
        )
      ),
      """{
        |  (`p`:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`bar`:`Person`) REQUIRE (`bar`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKC-2-7",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING NOT NULL}),
        |  CONSTRAINT FOR (bar :Person =>) REQUIRE bar.name IS KEY }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = false)
          )
        )),
        Seq(
          keyConstraint(identifyingNodeTypeRef("Person", "bar"), ArraySeq(prop("bar", "name")))
        )
      ),
      """{
        |  (`p`:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`bar`:`Person` =>) REQUIRE (`bar`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKC-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, birthday :: DATE, socialNo :: STRING}) REQUIRE (p.name, p.birthday) IS KEY }""",
      graphType(
        Seq(nodeTypeWithConstraints(
          "Person",
          "p",
          Set(KeyConstraint(ArraySeq(prop("p", "name"), prop("p", "birthday")))(defaultPos)),
          propertyType(
            "name",
            StringType(isNullable = true)
          ),
          propertyType(
            "birthday",
            DateType(isNullable = true)
          ),
          propertyType(
            "socialNo",
            StringType(isNullable = true)
          )
        )),
        Seq()
      ),
      """{
        |  (`p`:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}) REQUIRE (`p`.`name`, `p`.`birthday`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKC-3-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, birthday :: DATE, socialNo :: STRING}), CONSTRAINT FOR (p) REQUIRE (p.name, p.birthday) IS KEY }""",
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = true)
          ),
          propertyType(
            "birthday",
            DateType(isNullable = true)
          ),
          propertyType(
            "socialNo",
            StringType(isNullable = true)
          )
        )),
        Seq(
          keyConstraint(nodeTypeRefByVar("p"), ArraySeq(prop("p", "name"), prop("p", "birthday")))
        )
      ),
      """{
        |  (`p`:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`, `p`.`birthday`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKC-3-3",
      """ALTER CURRENT GRAPH TYPE SET { (  :Person => {name :: STRING, birthday :: DATE, socialNo :: STRING}),
        |  CONSTRAINT FOR (p :Person) REQUIRE (p.name, p.birthday) IS KEY }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          propertyType(
            "name",
            StringType(isNullable = true)
          ),
          propertyType(
            "birthday",
            DateType(isNullable = true)
          ),
          propertyType(
            "socialNo",
            StringType(isNullable = true)
          )
        )),
        Seq(
          keyConstraint(nodeTypeRefByLabel("Person", "p"), ArraySeq(prop("p", "name"), prop("p", "birthday")))
        )
      ),
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`p`:`Person`) REQUIRE (`p`.`name`, `p`.`birthday`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKC-3-4",
      """ALTER CURRENT GRAPH TYPE SET { (  :Person => {name :: STRING, birthday :: DATE, socialNo :: STRING}),
        |  CONSTRAINT FOR (p :Person =>) REQUIRE (p.name, p.birthday) IS KEY }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          propertyType(
            "name",
            StringType(isNullable = true)
          ),
          propertyType(
            "birthday",
            DateType(isNullable = true)
          ),
          propertyType(
            "socialNo",
            StringType(isNullable = true)
          )
        )),
        Seq(
          keyConstraint(identifyingNodeTypeRef("Person", "p"), ArraySeq(prop("p", "name"), prop("p", "birthday")))
        )
      ),
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`p`:`Person` =>) REQUIRE (`p`.`name`, `p`.`birthday`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKC-3-5",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, birthday :: DATE, socialNo :: STRING}),
        |  CONSTRAINT FOR (bar :Person) REQUIRE (bar.name, bar.birthday) IS KEY }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = true)
          ),
          propertyType(
            "birthday",
            DateType(isNullable = true)
          ),
          propertyType(
            "socialNo",
            StringType(isNullable = true)
          )
        )),
        Seq(
          keyConstraint(
            nodeTypeRefByLabel("Person", "bar"),
            ArraySeq(prop("bar", "name"), prop("bar", "birthday"))
          )
        )
      ),
      """{
        |  (`p`:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`bar`:`Person`) REQUIRE (`bar`.`name`, `bar`.`birthday`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKC-3-6",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, birthday :: DATE, socialNo :: STRING}),
        |  CONSTRAINT FOR (bar :Person =>) REQUIRE (bar.name, bar.birthday) IS KEY }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = true)
          ),
          propertyType(
            "birthday",
            DateType(isNullable = true)
          ),
          propertyType(
            "socialNo",
            StringType(isNullable = true)
          )
        )),
        Seq(
          keyConstraint(
            identifyingNodeTypeRef("Person", "bar"),
            ArraySeq(prop("bar", "name"), prop("bar", "birthday"))
          )
        )
      ),
      """{
        |  (`p`:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`bar`:`Person` =>) REQUIRE (`bar`.`name`, `bar`.`birthday`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NUC-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (  :Person => {name :: STRING IS UNIQUE}) }""",
      graphType(nodeType(
        "Person",
        propertyType(
          "name",
          StringType(isNullable = true),
          PropertyInlineUniquenessConstraint()(defaultPos)
        )
      )),
      """{
        |  (:`Person` => {`name` :: STRING IS UNIQUE})
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NUC-1-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING}) REQUIRE p.name IS UNIQUE }""",
      graphType(
        Seq(nodeTypeWithConstraints(
          "Person",
          "p",
          Set(UniquenessConstraint(ArraySeq(prop("p", "name")))(defaultPos)),
          propertyType(
            "name",
            StringType(isNullable = true)
          )
        )),
        Seq()
      ),
      """{
        |  (`p`:`Person` => {`name` :: STRING}) REQUIRE (`p`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NUC-1-3",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING}),
        |  CONSTRAINT FOR (p) REQUIRE p.name IS UNIQUE }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = true)
          )
        )),
        Seq(
          uniquenessConstraint(nodeTypeRefByVar("p"), ArraySeq(prop("p", "name")))
        )
      ),
      """{
        |  (`p`:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NUC-1-4",
      """ALTER CURRENT GRAPH TYPE SET { (  :Person => {name :: STRING}),
        |  CONSTRAINT FOR (p :Person) REQUIRE p.name IS UNIQUE }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          propertyType(
            "name",
            StringType(isNullable = true)
          )
        )),
        Seq(
          uniquenessConstraint(nodeTypeRefByLabel("Person", "p"), ArraySeq(prop("p", "name")))
        )
      ),
      """{
        |  (:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`p`:`Person`) REQUIRE (`p`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NUC-1-5",
      """ALTER CURRENT GRAPH TYPE SET { (  :Person => {name :: STRING}),
        |  CONSTRAINT FOR (p :Person =>) REQUIRE p.name IS UNIQUE }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          propertyType(
            "name",
            StringType(isNullable = true)
          )
        )),
        Seq(
          uniquenessConstraint(identifyingNodeTypeRef("Person", "p"), ArraySeq(prop("p", "name")))
        )
      ),
      """{
        |  (:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`p`:`Person` =>) REQUIRE (`p`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NUC-1-6",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING}),
        |  CONSTRAINT FOR (bar :Person) REQUIRE bar.name IS UNIQUE }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = true)
          )
        )),
        Seq(
          uniquenessConstraint(nodeTypeRefByLabel("Person", "bar"), ArraySeq(prop("bar", "name")))
        )
      ),
      """{
        |  (`p`:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`bar`:`Person`) REQUIRE (`bar`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NUC-1-7",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING}),
        |  CONSTRAINT FOR (bar :Person =>) REQUIRE bar.name IS UNIQUE }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = true)
          )
        )),
        Seq(
          uniquenessConstraint(identifyingNodeTypeRef("Person", "bar"), ArraySeq(prop("bar", "name")))
        )
      ),
      """{
        |  (`p`:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`bar`:`Person` =>) REQUIRE (`bar`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NUC-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (  :Person => {name :: STRING NOT NULL IS UNIQUE}) }""",
      graphType(
        nodeType(
          "Person",
          propertyType(
            "name",
            StringType(isNullable = false),
            PropertyInlineUniquenessConstraint()(defaultPos)
          )
        )
      ),
      """{
        |  (:`Person` => {`name` :: STRING NOT NULL IS UNIQUE})
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NUC-2-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING NOT NULL}) REQUIRE p.name IS UNIQUE }""",
      graphType(
        nodeTypeWithConstraints(
          "Person",
          "p",
          Set(UniquenessConstraint(ArraySeq(prop("p", "name")))(defaultPos)),
          propertyType(
            "name",
            StringType(isNullable = false)
          )
        )
      ),
      """{
        |  (`p`:`Person` => {`name` :: STRING NOT NULL}) REQUIRE (`p`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NUC-2-3",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING NOT NULL}), CONSTRAINT FOR (p) REQUIRE p.name IS UNIQUE }""",
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = false)
          )
        )),
        Seq(
          uniquenessConstraint(nodeTypeRefByVar("p"), ArraySeq(prop("p", "name")))
        )
      ),
      """{
        |  (`p`:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NUC-2-4",
      """ALTER CURRENT GRAPH TYPE SET { (  :Person => {name :: STRING NOT NULL}),
        |  CONSTRAINT FOR (p :Person) REQUIRE p.name IS UNIQUE }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          propertyType(
            "name",
            StringType(isNullable = false)
          )
        )),
        Seq(
          uniquenessConstraint(nodeTypeRefByLabel("Person", "p"), ArraySeq(prop("p", "name")))
        )
      ),
      """{
        |  (:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`p`:`Person`) REQUIRE (`p`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NUC-2-5",
      """ALTER CURRENT GRAPH TYPE SET { (  :Person => {name :: STRING NOT NULL}),
        |  CONSTRAINT FOR (p :Person =>) REQUIRE p.name IS UNIQUE }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          propertyType(
            "name",
            StringType(isNullable = false)
          )
        )),
        Seq(
          uniquenessConstraint(identifyingNodeTypeRef("Person", "p"), ArraySeq(prop("p", "name")))
        )
      ),
      """{
        |  (:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`p`:`Person` =>) REQUIRE (`p`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NUC-2-6",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING NOT NULL}), CONSTRAINT FOR (bar :Person) REQUIRE bar.name IS UNIQUE }""",
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = false)
          )
        )),
        Seq(
          uniquenessConstraint(nodeTypeRefByLabel("Person", "bar"), ArraySeq(prop("bar", "name")))
        )
      ),
      """{
        |  (`p`:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`bar`:`Person`) REQUIRE (`bar`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NUC-2-7",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING NOT NULL}), CONSTRAINT FOR (bar :Person =>) REQUIRE bar.name IS UNIQUE }""",
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = false)
          )
        )),
        Seq(
          uniquenessConstraint(identifyingNodeTypeRef("Person", "bar"), ArraySeq(prop("bar", "name")))
        )
      ),
      """{
        |  (`p`:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`bar`:`Person` =>) REQUIRE (`bar`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`name` :: STRING NOT NULL}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NUC-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, birthday :: DATE, socialNo :: STRING}) REQUIRE (p.name, p.birthday) IS UNIQUE }""",
      graphType(
        Seq(nodeTypeWithConstraints(
          "Person",
          "p",
          Set(UniquenessConstraint(ArraySeq(prop("p", "name"), prop("p", "birthday")))(defaultPos)),
          propertyType(
            "name",
            StringType(isNullable = true)
          ),
          propertyType(
            "birthday",
            DateType(isNullable = true)
          ),
          propertyType(
            "socialNo",
            StringType(isNullable = true)
          )
        )),
        Seq()
      ),
      """{
        |  (`p`:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}) REQUIRE (`p`.`name`, `p`.`birthday`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NUC-3-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, birthday :: DATE, socialNo :: STRING}), CONSTRAINT FOR (p) REQUIRE (p.name, p.birthday) IS UNIQUE }""",
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = true)
          ),
          propertyType(
            "birthday",
            DateType(isNullable = true)
          ),
          propertyType(
            "socialNo",
            StringType(isNullable = true)
          )
        )),
        Seq(
          uniquenessConstraint(nodeTypeRefByVar("p"), ArraySeq(prop("p", "name"), prop("p", "birthday")))
        )
      ),
      """{
        |  (`p`:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`, `p`.`birthday`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NUC-3-3",
      """ALTER CURRENT GRAPH TYPE SET { (  :Person => {name :: STRING, birthday :: DATE, socialNo :: STRING}),
        |  CONSTRAINT FOR (p :Person) REQUIRE (p.name, p.birthday) IS UNIQUE }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          propertyType(
            "name",
            StringType(isNullable = true)
          ),
          propertyType(
            "birthday",
            DateType(isNullable = true)
          ),
          propertyType(
            "socialNo",
            StringType(isNullable = true)
          )
        )),
        Seq(
          uniquenessConstraint(
            nodeTypeRefByLabel("Person", "p"),
            ArraySeq(prop("p", "name"), prop("p", "birthday"))
          )
        )
      ),
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`p`:`Person`) REQUIRE (`p`.`name`, `p`.`birthday`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NUC-3-4",
      """ALTER CURRENT GRAPH TYPE SET { (  :Person => {name :: STRING, birthday :: DATE, socialNo :: STRING}),
        |  CONSTRAINT FOR (p :Person =>) REQUIRE (p.name, p.birthday) IS UNIQUE }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          propertyType(
            "name",
            StringType(isNullable = true)
          ),
          propertyType(
            "birthday",
            DateType(isNullable = true)
          ),
          propertyType(
            "socialNo",
            StringType(isNullable = true)
          )
        )),
        Seq(
          uniquenessConstraint(
            identifyingNodeTypeRef("Person", "p"),
            ArraySeq(prop("p", "name"), prop("p", "birthday"))
          )
        )
      ),
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`p`:`Person` =>) REQUIRE (`p`.`name`, `p`.`birthday`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NUC-3-5",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, birthday :: DATE, socialNo :: STRING}),
        |  CONSTRAINT FOR (bar :Person) REQUIRE (bar.name, bar.birthday) IS UNIQUE }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = true)
          ),
          propertyType(
            "birthday",
            DateType(isNullable = true)
          ),
          propertyType(
            "socialNo",
            StringType(isNullable = true)
          )
        )),
        Seq(
          uniquenessConstraint(
            nodeTypeRefByLabel("Person", "bar"),
            ArraySeq(prop("bar", "name"), prop("bar", "birthday"))
          )
        )
      ),
      """{
        |  (`p`:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`bar`:`Person`) REQUIRE (`bar`.`name`, `bar`.`birthday`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NUC-3-6",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, birthday :: DATE, socialNo :: STRING}),
        |  CONSTRAINT FOR (bar :Person =>) REQUIRE (bar.name, bar.birthday) IS UNIQUE }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = true)
          ),
          propertyType(
            "birthday",
            DateType(isNullable = true)
          ),
          propertyType(
            "socialNo",
            StringType(isNullable = true)
          )
        )),
        Seq(
          uniquenessConstraint(
            identifyingNodeTypeRef("Person", "bar"),
            ArraySeq(prop("bar", "name"), prop("bar", "birthday"))
          )
        )
      ),
      """{
        |  (`p`:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`bar`:`Person` =>) REQUIRE (`bar`.`name`, `bar`.`birthday`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKUC-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, birthday :: DATE, socialNo :: STRING})
        |  REQUIRE (p.name, p.birthday) IS KEY
        |  REQUIRE (p.socialNo) IS KEY }""".stripMargin,
      graphType(
        Seq(nodeTypeWithConstraints(
          "Person",
          "p",
          Set(
            KeyConstraint(ArraySeq(prop("p", "name"), prop("p", "birthday")))(defaultPos),
            KeyConstraint(ArraySeq(prop("p", "socialNo")))(defaultPos)
          ),
          propertyType(
            "name",
            StringType(isNullable = true)
          ),
          propertyType(
            "birthday",
            DateType(isNullable = true)
          ),
          propertyType(
            "socialNo",
            StringType(isNullable = true)
          )
        )),
        Seq()
      ),
      """{
        |  (`p`:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}) REQUIRE (`p`.`name`, `p`.`birthday`) IS KEY REQUIRE (`p`.`socialNo`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`socialNo`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKUC-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, birthday :: DATE, socialNo :: STRING})
        |  REQUIRE (p.name, p.birthday) IS UNIQUE
        |  REQUIRE (p.socialNo) IS UNIQUE }""".stripMargin,
      graphType(
        Seq(nodeTypeWithConstraints(
          "Person",
          "p",
          Set(
            UniquenessConstraint(ArraySeq(prop("p", "name"), prop("p", "birthday")))(defaultPos),
            UniquenessConstraint(ArraySeq(prop("p", "socialNo")))(defaultPos)
          ),
          propertyType(
            "name",
            StringType(isNullable = true)
          ),
          propertyType(
            "birthday",
            DateType(isNullable = true)
          ),
          propertyType(
            "socialNo",
            StringType(isNullable = true)
          )
        )),
        Seq()
      ),
      """{
        |  (`p`:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}) REQUIRE (`p`.`name`, `p`.`birthday`) IS UNIQUE REQUIRE (`p`.`socialNo`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS UNIQUE,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`socialNo`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKUC-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, birthday :: DATE, socialNo :: STRING})
        |  REQUIRE (p.name, p.birthday) IS KEY
        |  REQUIRE (p.socialNo) IS UNIQUE }""".stripMargin,
      graphType(
        Seq(nodeTypeWithConstraints(
          "Person",
          "p",
          Set(
            KeyConstraint(ArraySeq(prop("p", "name"), prop("p", "birthday")))(defaultPos),
            UniquenessConstraint(ArraySeq(prop("p", "socialNo")))(defaultPos)
          ),
          propertyType(
            "name",
            StringType(isNullable = true)
          ),
          propertyType(
            "birthday",
            DateType(isNullable = true)
          ),
          propertyType(
            "socialNo",
            StringType(isNullable = true)
          )
        )),
        Seq()
      ),
      """{
        |  (`p`:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}) REQUIRE (`p`.`name`, `p`.`birthday`) IS KEY REQUIRE (`p`.`socialNo`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`socialNo`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKUC-3-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, birthday :: DATE, socialNo :: STRING})
        |  REQUIRE (p.socialNo) IS UNIQUE
        |  REQUIRE (p.name, p.birthday) IS KEY }""".stripMargin,
      graphType(
        Seq(nodeTypeWithConstraints(
          "Person",
          "p",
          Set(
            UniquenessConstraint(ArraySeq(prop("p", "socialNo")))(defaultPos),
            KeyConstraint(ArraySeq(prop("p", "name"), prop("p", "birthday")))(defaultPos)
          ),
          propertyType(
            "name",
            StringType(isNullable = true)
          ),
          propertyType(
            "birthday",
            DateType(isNullable = true)
          ),
          propertyType(
            "socialNo",
            StringType(isNullable = true)
          )
        )),
        Seq()
      ),
      """{
        |  (`p`:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}) REQUIRE (`p`.`name`, `p`.`birthday`) IS KEY REQUIRE (`p`.`socialNo`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`socialNo`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKUC-3-3",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, birthday :: DATE, socialNo :: STRING}),
        |  CONSTRAINT FOR (p) REQUIRE (p.name, p.birthday) IS KEY,
        |  CONSTRAINT FOR (p) REQUIRE (p.socialNo) IS UNIQUE }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = true)
          ),
          propertyType(
            "birthday",
            DateType(isNullable = true)
          ),
          propertyType(
            "socialNo",
            StringType(isNullable = true)
          )
        )),
        Seq(
          keyConstraint(
            nodeTypeRefByVar("p"),
            ArraySeq(prop("p", "name"), prop("p", "birthday"))
          ),
          uniquenessConstraint(
            nodeTypeRefByVar("p"),
            ArraySeq(prop("p", "socialNo"))
          )
        )
      ),
      """{
        |  (`p`:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`, `p`.`birthday`) IS KEY,
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`socialNo`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`socialNo`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKUC-3-4",
      """ALTER CURRENT GRAPH TYPE SET { (  :Person => {name :: STRING, birthday :: DATE, socialNo :: STRING}),
        |  CONSTRAINT FOR (p :Person) REQUIRE (p.name, p.birthday) IS KEY,
        |  CONSTRAINT FOR (x :Person) REQUIRE x.socialNo IS UNIQUE }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          propertyType(
            "name",
            StringType(isNullable = true)
          ),
          propertyType(
            "birthday",
            DateType(isNullable = true)
          ),
          propertyType(
            "socialNo",
            StringType(isNullable = true)
          )
        )),
        Seq(
          keyConstraint(
            nodeTypeRefByLabel("Person", "p"),
            ArraySeq(prop("p", "name"), prop("p", "birthday"))
          ),
          uniquenessConstraint(
            nodeTypeRefByLabel("Person", "x"),
            ArraySeq(prop("x", "socialNo"))
          )
        )
      ),
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`p`:`Person`) REQUIRE (`p`.`name`, `p`.`birthday`) IS KEY,
        |  CONSTRAINT FOR (`x`:`Person`) REQUIRE (`x`.`socialNo`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`socialNo`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKUC-3-5",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, birthday :: DATE, socialNo :: STRING}),
        |  CONSTRAINT FOR (bar :Person) REQUIRE (bar.name, bar.birthday) IS KEY,
        |  CONSTRAINT FOR (x :Person) REQUIRE x.socialNo IS UNIQUE }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = true)
          ),
          propertyType(
            "birthday",
            DateType(isNullable = true)
          ),
          propertyType(
            "socialNo",
            StringType(isNullable = true)
          )
        )),
        Seq(
          keyConstraint(
            nodeTypeRefByLabel("Person", "bar"),
            ArraySeq(prop("bar", "name"), prop("bar", "birthday"))
          ),
          uniquenessConstraint(
            nodeTypeRefByLabel("Person", "x"),
            ArraySeq(prop("x", "socialNo"))
          )
        )
      ),
      """{
        |  (`p`:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`bar`:`Person`) REQUIRE (`bar`.`name`, `bar`.`birthday`) IS KEY,
        |  CONSTRAINT FOR (`x`:`Person`) REQUIRE (`x`.`socialNo`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`socialNo`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKUC-3-6",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, birthday :: DATE, socialNo :: STRING}),
        |  CONSTRAINT FOR (x :Person) REQUIRE x.socialNo IS UNIQUE,
        |  CONSTRAINT FOR (bar :Person) REQUIRE (bar.name, bar.birthday) IS KEY }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = true)
          ),
          propertyType(
            "birthday",
            DateType(isNullable = true)
          ),
          propertyType(
            "socialNo",
            StringType(isNullable = true)
          )
        )),
        Seq(
          uniquenessConstraint(
            nodeTypeRefByLabel("Person", "x"),
            ArraySeq(prop("x", "socialNo"))
          ),
          keyConstraint(
            nodeTypeRefByLabel("Person", "bar"),
            ArraySeq(prop("bar", "name"), prop("bar", "birthday"))
          )
        )
      ),
      """{
        |  (`p`:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`bar`:`Person`) REQUIRE (`bar`.`name`, `bar`.`birthday`) IS KEY,
        |  CONSTRAINT FOR (`x`:`Person`) REQUIRE (`x`.`socialNo`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`socialNo`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SNT-PE-NKUC-3-7",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, birthday :: DATE, socialNo :: STRING}),
        |  CONSTRAINT FOR (x :Person =>) REQUIRE x.socialNo IS UNIQUE,
        |  CONSTRAINT FOR (bar :Person =>) REQUIRE (bar.name, bar.birthday) IS KEY }""".stripMargin,
      graphType(
        Seq(nodeType(
          "Person",
          "p",
          propertyType(
            "name",
            StringType(isNullable = true)
          ),
          propertyType(
            "birthday",
            DateType(isNullable = true)
          ),
          propertyType(
            "socialNo",
            StringType(isNullable = true)
          )
        )),
        Seq(
          uniquenessConstraint(
            identifyingNodeTypeRef("Person", "x"),
            ArraySeq(prop("x", "socialNo"))
          ),
          keyConstraint(
            identifyingNodeTypeRef("Person", "bar"),
            ArraySeq(prop("bar", "name"), prop("bar", "birthday"))
          )
        )
      ),
      """{
        |  (`p`:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`bar`:`Person` =>) REQUIRE (`bar`.`name`, `bar`.`birthday`) IS KEY,
        |  CONSTRAINT FOR (`x`:`Person` =>) REQUIRE (`x`.`socialNo`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`birthday` :: DATE, `name` :: STRING, `socialNo` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`birthday`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`socialNo`) IS UNIQUE
        |}""".stripMargin
    )
  )

  private def mnt(): Seq[GraphTypeTestCase] = Seq(
    GraphTypeTestCase(
      "MNT-PE-S-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => :Named), (:City => {name :: STRING}) }""",
      graphType(
        nodeType(
          "Person",
          Set("Named")
        ),
        nodeType(
          "City",
          propertyType("name", StringType(isNullable = true))
        )
      ),
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => :`Named`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => :`Named`)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MNT-PE-S-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => :Named),
        |  (:City => {name :: STRING}),
        |  (:River => {length :: INT}) }""".stripMargin,
      graphType(
        nodeType(
          "Person",
          Set("Named")
        ),
        nodeType(
          "City",
          propertyType("name", StringType(isNullable = true))
        ),
        nodeType(
          "River",
          propertyType("length", IntegerType(isNullable = true))
        )
      ),
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => :`Named`),
        |  (:`River` => {`length` :: INTEGER})
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => :`Named`),
        |  (:`River` => {`length` :: INTEGER})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MNT-PE-S-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => :Named),
        |  (:City => {name :: STRING}),
        |  (:River => {length :: INT}),
        |  (:Mountain => {height :: INT, weight :: INT}),
        |  (:Country => {population :: INT, grossProduct :: FLOAT}) }""".stripMargin,
      graphType(
        nodeType(
          "Person",
          Set("Named")
        ),
        nodeType(
          "City",
          propertyType("name", StringType(isNullable = true))
        ),
        nodeType(
          "River",
          propertyType("length", IntegerType(isNullable = true))
        ),
        nodeType(
          "Mountain",
          propertyType("height", IntegerType(isNullable = true)),
          propertyType("weight", IntegerType(isNullable = true))
        ),
        nodeType(
          "Country",
          propertyType("population", IntegerType(isNullable = true)),
          propertyType("grossProduct", FloatType(isNullable = true))
        )
      ),
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Country` => {`grossProduct` :: FLOAT, `population` :: INTEGER}),
        |  (:`Mountain` => {`height` :: INTEGER, `weight` :: INTEGER}),
        |  (:`Person` => :`Named`),
        |  (:`River` => {`length` :: INTEGER})
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Country` => {`grossProduct` :: FLOAT, `population` :: INTEGER}),
        |  (:`Mountain` => {`height` :: INTEGER, `weight` :: INTEGER}),
        |  (:`Person` => :`Named`),
        |  (:`River` => {`length` :: INTEGER})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MNT-PE-CIL-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => :Named), (:City => :Named) }""",
      graphType(
        nodeType(
          "Person",
          Set("Named")
        ),
        nodeType(
          "City",
          Set("Named")
        )
      ),
      """{
        |  (:`City` => :`Named`),
        |  (:`Person` => :`Named`)
        |}""".stripMargin,
      """{
        |  (:`City` => :`Named`),
        |  (:`Person` => :`Named`)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MNT-PE-CIL-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => :Named&Tagged), (:City => :Named&Tagged) }""",
      graphType(
        nodeType(
          "Person",
          Set("Named", "Tagged")
        ),
        nodeType(
          "City",
          Set("Named", "Tagged")
        )
      ),
      """{
        |  (:`City` => :`Named`&`Tagged`),
        |  (:`Person` => :`Named`&`Tagged`)
        |}""".stripMargin,
      """{
        |  (:`City` => :`Named`&`Tagged`),
        |  (:`Person` => :`Named`&`Tagged`)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MNT-PE-CIL-2-2",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => :Named&Tagged), (:City => :Tagged&Named) }""",
      graphType(
        nodeType(
          "Person",
          Set("Named", "Tagged")
        ),
        nodeType(
          "City",
          Set("Named", "Tagged")
        )
      ),
      """{
        |  (:`City` => :`Named`&`Tagged`),
        |  (:`Person` => :`Named`&`Tagged`)
        |}""".stripMargin,
      """{
        |  (:`City` => :`Named`&`Tagged`),
        |  (:`Person` => :`Named`&`Tagged`)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MNT-PE-CIL-2-3",
      """ALTER CURRENT GRAPH TYPE SET { (:City => :Tagged&Named), (:Person => :Named&Tagged) }""",
      graphType(
        nodeType(
          "City",
          Set("Named", "Tagged")
        ),
        nodeType(
          "Person",
          Set("Named", "Tagged")
        )
      ),
      """{
        |  (:`City` => :`Named`&`Tagged`),
        |  (:`Person` => :`Named`&`Tagged`)
        |}""".stripMargin,
      """{
        |  (:`City` => :`Named`&`Tagged`),
        |  (:`Person` => :`Named`&`Tagged`)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MNT-PE-CIL-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => :Named {age :: INT}),(:City => :Named {population :: INT}) }""",
      graphType(
        nodeType(
          "Person",
          Set("Named"),
          propertyType("age", IntegerType(isNullable = true))
        ),
        nodeType(
          "City",
          Set("Named"),
          propertyType("population", IntegerType(isNullable = true))
        )
      ),
      """{
        |  (:`City` => :`Named` {`population` :: INTEGER}),
        |  (:`Person` => :`Named` {`age` :: INTEGER})
        |}""".stripMargin,
      """{
        |  (:`City` => :`Named` {`population` :: INTEGER}),
        |  (:`Person` => :`Named` {`age` :: INTEGER})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MNT-PE-CIP-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => {name :: STRING}),(:City => {name :: STRING}) }""",
      graphType(
        nodeType(
          "Person",
          propertyType("name", StringType(isNullable = true))
        ),
        nodeType(
          "City",
          propertyType("name", StringType(isNullable = true))
        )
      ),
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`name` :: STRING})
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`name` :: STRING})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MNT-PE-CIP-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => {name :: STRING, tag :: STRING}), (:City => {name :: STRING, tag :: STRING}) }""",
      graphType(
        nodeType(
          "Person",
          propertyType("name", StringType(isNullable = true)),
          propertyType("tag", StringType(isNullable = true))
        ),
        nodeType(
          "City",
          propertyType("name", StringType(isNullable = true)),
          propertyType("tag", StringType(isNullable = true))
        )
      ),
      """{
        |  (:`City` => {`name` :: STRING, `tag` :: STRING}),
        |  (:`Person` => {`name` :: STRING, `tag` :: STRING})
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING, `tag` :: STRING}),
        |  (:`Person` => {`name` :: STRING, `tag` :: STRING})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MNT-PE-CIP-2-2",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => {name :: STRING, tag :: STRING}), (:City => {tag :: STRING, name :: STRING}) }""",
      graphType(
        nodeType(
          "Person",
          propertyType("name", StringType(isNullable = true)),
          propertyType("tag", StringType(isNullable = true))
        ),
        nodeType(
          "City",
          propertyType("name", StringType(isNullable = true)),
          propertyType("tag", StringType(isNullable = true))
        )
      ),
      """{
        |  (:`City` => {`name` :: STRING, `tag` :: STRING}),
        |  (:`Person` => {`name` :: STRING, `tag` :: STRING})
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING, `tag` :: STRING}),
        |  (:`Person` => {`name` :: STRING, `tag` :: STRING})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MNT-PE-CIP-2-3",
      """ALTER CURRENT GRAPH TYPE SET { (:City => {tag :: STRING, name :: STRING}), (:Person => {name :: STRING, tag :: STRING}) }""",
      graphType(
        nodeType(
          "City",
          propertyType("name", StringType(isNullable = true)),
          propertyType("tag", StringType(isNullable = true))
        ),
        nodeType(
          "Person",
          propertyType("name", StringType(isNullable = true)),
          propertyType("tag", StringType(isNullable = true))
        )
      ),
      """{
        |  (:`City` => {`name` :: STRING, `tag` :: STRING}),
        |  (:`Person` => {`name` :: STRING, `tag` :: STRING})
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING, `tag` :: STRING}),
        |  (:`Person` => {`name` :: STRING, `tag` :: STRING})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MNT-PE-CILP-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => :Named {name :: STRING}), (:City => :Named {name :: STRING}) }""",
      graphType(
        nodeType(
          "Person",
          Set("Named"),
          propertyType("name", StringType(isNullable = true))
        ),
        nodeType(
          "City",
          Set("Named"),
          propertyType("name", StringType(isNullable = true))
        )
      ),
      """{
        |  (:`City` => :`Named` {`name` :: STRING}),
        |  (:`Person` => :`Named` {`name` :: STRING})
        |}""".stripMargin,
      """{
        |  (:`City` => :`Named` {`name` :: STRING}),
        |  (:`Person` => :`Named` {`name` :: STRING})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MNT-PE-CILP-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => :Named&Tagged {name :: STRING, tag :: STRING}), (:City => :Named&Tagged {name :: STRING, tag :: STRING}) }""",
      graphType(
        nodeType(
          "Person",
          Set("Named", "Tagged"),
          propertyType("name", StringType(isNullable = true)),
          propertyType("tag", StringType(isNullable = true))
        ),
        nodeType(
          "City",
          Set("Named", "Tagged"),
          propertyType("name", StringType(isNullable = true)),
          propertyType("tag", StringType(isNullable = true))
        )
      ),
      """{
        |  (:`City` => :`Named`&`Tagged` {`name` :: STRING, `tag` :: STRING}),
        |  (:`Person` => :`Named`&`Tagged` {`name` :: STRING, `tag` :: STRING})
        |}""".stripMargin,
      """{
        |  (:`City` => :`Named`&`Tagged` {`name` :: STRING, `tag` :: STRING}),
        |  (:`Person` => :`Named`&`Tagged` {`name` :: STRING, `tag` :: STRING})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MNT-PE-CILP-2-2",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => :Tagged&Named {name :: STRING, tag :: STRING}), (:City => :Named&Tagged {tag :: STRING, name :: STRING}) }""",
      graphType(
        nodeType(
          "Person",
          Set("Named", "Tagged"),
          propertyType("name", StringType(isNullable = true)),
          propertyType("tag", StringType(isNullable = true))
        ),
        nodeType(
          "City",
          Set("Named", "Tagged"),
          propertyType("name", StringType(isNullable = true)),
          propertyType("tag", StringType(isNullable = true))
        )
      ),
      """{
        |  (:`City` => :`Named`&`Tagged` {`name` :: STRING, `tag` :: STRING}),
        |  (:`Person` => :`Named`&`Tagged` {`name` :: STRING, `tag` :: STRING})
        |}""".stripMargin,
      """{
        |  (:`City` => :`Named`&`Tagged` {`name` :: STRING, `tag` :: STRING}),
        |  (:`Person` => :`Named`&`Tagged` {`name` :: STRING, `tag` :: STRING})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MNT-PE-CILP-2-3",
      """ALTER CURRENT GRAPH TYPE SET { (:City => :Tagged&Named {tag :: STRING, name :: STRING}), (:Person => :Named&Tagged {name :: STRING, tag :: STRING}) }""",
      graphType(
        nodeType(
          "City",
          Set("Named", "Tagged"),
          propertyType("name", StringType(isNullable = true)),
          propertyType("tag", StringType(isNullable = true))
        ),
        nodeType(
          "Person",
          Set("Named", "Tagged"),
          propertyType("name", StringType(isNullable = true)),
          propertyType("tag", StringType(isNullable = true))
        )
      ),
      """{
        |  (:`City` => :`Named`&`Tagged` {`name` :: STRING, `tag` :: STRING}),
        |  (:`Person` => :`Named`&`Tagged` {`name` :: STRING, `tag` :: STRING})
        |}""".stripMargin,
      """{
        |  (:`City` => :`Named`&`Tagged` {`name` :: STRING, `tag` :: STRING}),
        |  (:`Person` => :`Named`&`Tagged` {`name` :: STRING, `tag` :: STRING})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MNT-PE-CILP-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => :Tagged&Named {name :: STRING, tag :: STRING, age :: INT}),
        |  (:City => :Named&Tagged&Populated {tag :: STRING, population :: INT, name :: STRING}) }""".stripMargin,
      graphType(
        nodeType(
          "Person",
          Set("Named", "Tagged"),
          propertyType("name", StringType(isNullable = true)),
          propertyType("tag", StringType(isNullable = true)),
          propertyType("age", IntegerType(isNullable = true))
        ),
        nodeType(
          "City",
          Set("Named", "Tagged", "Populated"),
          propertyType("name", StringType(isNullable = true)),
          propertyType("population", IntegerType(isNullable = true)),
          propertyType("tag", StringType(isNullable = true))
        )
      ),
      """{
        |  (:`City` => :`Named`&`Populated`&`Tagged` {`name` :: STRING, `population` :: INTEGER, `tag` :: STRING}),
        |  (:`Person` => :`Named`&`Tagged` {`age` :: INTEGER, `name` :: STRING, `tag` :: STRING})
        |}""".stripMargin,
      """{
        |  (:`City` => :`Named`&`Populated`&`Tagged` {`name` :: STRING, `population` :: INTEGER, `tag` :: STRING}),
        |  (:`Person` => :`Named`&`Tagged` {`age` :: INTEGER, `name` :: STRING, `tag` :: STRING})
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MNT-PE-KUC-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => {name :: STRING IS KEY, age :: INT}), (:City => {name :: STRING IS KEY, population :: INT}) }""",
      graphType(
        nodeType(
          "Person",
          propertyType("name", StringType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos)),
          propertyType("age", IntegerType(isNullable = true))
        ),
        nodeType(
          "City",
          propertyType("name", StringType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos)),
          propertyType("population", IntegerType(isNullable = true))
        )
      ),
      """{
        |  (:`City` => {`name` :: STRING IS KEY, `population` :: INTEGER}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING IS KEY})
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`City` =>) REQUIRE (`n`.`name`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MNT-PE-KUC-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT})
        |  REQUIRE (p.name, p.age) IS UNIQUE,
        |  (c :City => {name :: STRING, population :: INT})
        |  REQUIRE (c.name, c.population) IS UNIQUE }""".stripMargin,
      graphType(
        nodeTypeWithConstraints(
          "Person",
          "p",
          Set(UniquenessConstraint(ArraySeq(prop("p", "name"), prop("p", "age")))(defaultPos)),
          propertyType("name", StringType(isNullable = true)),
          propertyType("age", IntegerType(isNullable = true))
        ),
        nodeTypeWithConstraints(
          "City",
          "c",
          Set(UniquenessConstraint(ArraySeq(prop("c", "name"), prop("c", "population")))(defaultPos)),
          propertyType("name", StringType(isNullable = true)),
          propertyType("population", IntegerType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING, `population` :: INTEGER}) REQUIRE (`c`.`name`, `c`.`population`) IS UNIQUE,
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}) REQUIRE (`p`.`name`, `p`.`age`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`City` =>) REQUIRE (`n`.`name`, `n`.`population`) IS UNIQUE,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`age`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MNT-PE-KUC-2-2",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => {name :: STRING, age :: INT}),
        |  (:City => {name :: STRING, population :: INT}),
        |  CONSTRAINT FOR (x:Person =>) REQUIRE (x.name, x.age) IS UNIQUE,
        |  CONSTRAINT FOR (x:City =>) REQUIRE (x.name, x.population) IS UNIQUE }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          ),
          nodeType(
            "City",
            propertyType("name", StringType(isNullable = true)),
            propertyType("population", IntegerType(isNullable = true))
          )
        ),
        Seq(
          uniquenessConstraint(
            identifyingNodeTypeRef("Person", "x"),
            ArraySeq(prop("x", "name"), prop("x", "age"))
          ),
          uniquenessConstraint(
            identifyingNodeTypeRef("City", "x"),
            ArraySeq(prop("x", "name"), prop("x", "population"))
          )
        )
      ),
      """{
        |  (:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`x`:`City` =>) REQUIRE (`x`.`name`, `x`.`population`) IS UNIQUE,
        |  CONSTRAINT FOR (`x`:`Person` =>) REQUIRE (`x`.`name`, `x`.`age`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`City` =>) REQUIRE (`n`.`name`, `n`.`population`) IS UNIQUE,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`age`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MNT-PE-KUC-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => {name :: STRING, age :: INT, ssn :: STRING}),
        |  (c:City => {name :: STRING, zip :: STRING, population :: INT}),
        |  CONSTRAINT FOR (c) REQUIRE (c.zip) IS KEY,
        |  CONSTRAINT FOR (x:Person =>) REQUIRE (x.name, x.age) IS UNIQUE,
        |  CONSTRAINT FOR (x:City =>) REQUIRE (x.name, x.population) IS UNIQUE,
        |  CONSTRAINT FOR (x:Person =>) REQUIRE (x.ssn) IS KEY }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true)),
            propertyType("ssn", StringType(isNullable = true))
          ),
          nodeType(
            "City",
            "c",
            propertyType("name", StringType(isNullable = true)),
            propertyType("zip", StringType(isNullable = true)),
            propertyType("population", IntegerType(isNullable = true))
          )
        ),
        Seq(
          keyConstraint(nodeTypeRefByVar("c"), ArraySeq(prop("c", "zip"))),
          uniquenessConstraint(
            identifyingNodeTypeRef("Person", "x"),
            ArraySeq(prop("x", "name"), prop("x", "age"))
          ),
          uniquenessConstraint(
            identifyingNodeTypeRef("City", "x"),
            ArraySeq(prop("x", "name"), prop("x", "population"))
          ),
          keyConstraint(identifyingNodeTypeRef("Person", "x"), ArraySeq(prop("x", "ssn")))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING, `population` :: INTEGER, `zip` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING, `ssn` :: STRING}),
        |  CONSTRAINT FOR (`x`:`City` =>) REQUIRE (`x`.`name`, `x`.`population`) IS UNIQUE,
        |  CONSTRAINT FOR (`x`:`Person` =>) REQUIRE (`x`.`name`, `x`.`age`) IS UNIQUE,
        |  CONSTRAINT FOR (`x`:`Person` =>) REQUIRE (`x`.`ssn`) IS KEY,
        |  CONSTRAINT FOR (`c`) REQUIRE (`c`.`zip`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING, `population` :: INTEGER, `zip` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING, `ssn` :: STRING}),
        |  CONSTRAINT FOR (`n`:`City` =>) REQUIRE (`n`.`name`, `n`.`population`) IS UNIQUE,
        |  CONSTRAINT FOR (`n`:`City` =>) REQUIRE (`n`.`zip`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`age`) IS UNIQUE,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`ssn`) IS KEY
        |}""".stripMargin
    )
  )

  private def set(): Seq[GraphTypeTestCase] = Seq(
    GraphTypeTestCase(
      "SET-PE-EI-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING, population :: INT}),
        |  (p)-[:LIVES_IN =>]->(c) }""".stripMargin,
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true)),
          propertyType("age", IntegerType(isNullable = true))
        ),
        nodeType(
          "City",
          "c",
          propertyType("name", StringType(isNullable = true)),
          propertyType("population", IntegerType(isNullable = true))
        ),
        edgeType(nodeTypeRefByVar("p"), "LIVES_IN", nodeTypeRefByVar("c"))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[:`LIVES_IN` =>]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` =>]->(:`City` =>)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EI-1-2",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => {name :: STRING, age :: INT}),
        |  (:City => {name :: STRING, population :: INT}),
        |  (:Person)-[:LIVES_IN =>]->(:City) }""".stripMargin,
      graphType(
        nodeType(
          "Person",
          propertyType("name", StringType(isNullable = true)),
          propertyType("age", IntegerType(isNullable = true))
        ),
        nodeType(
          "City",
          propertyType("name", StringType(isNullable = true)),
          propertyType("population", IntegerType(isNullable = true))
        ),
        edgeType(nodeTypeRefByLabel("Person"), "LIVES_IN", nodeTypeRefByLabel("City"))
      ),
      """{
        |  (:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person`)-[:`LIVES_IN` =>]->(:`City`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` =>]->(:`City` =>)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EI-1-3",
      """ALTER CURRENT GRAPH TYPE SET { (:Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING, population :: INT}),
        |  (:Person => )-[:LIVES_IN =>]->(c) }""".stripMargin,
      graphType(
        nodeType(
          "Person",
          propertyType("name", StringType(isNullable = true)),
          propertyType("age", IntegerType(isNullable = true))
        ),
        nodeType(
          "City",
          "c",
          propertyType("name", StringType(isNullable = true)),
          propertyType("population", IntegerType(isNullable = true))
        ),
        edgeType(identifyingNodeTypeRef("Person"), "LIVES_IN", nodeTypeRefByVar("c"))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` =>]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` =>]->(:`City` =>)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EPI-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING, population :: INT}),
        |  (p)-[:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true)),
          propertyType("age", IntegerType(isNullable = true))
        ),
        nodeType(
          "City",
          "c",
          propertyType("name", StringType(isNullable = true)),
          propertyType("population", IntegerType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EPI-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING, population :: INT}),
        |  (p)-[:LIVES_IN => {since :: DATE, address :: STRING NOT NULL}]->(c) }""".stripMargin,
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true)),
          propertyType("age", IntegerType(isNullable = true))
        ),
        nodeType(
          "City",
          "c",
          propertyType("name", StringType(isNullable = true)),
          propertyType("population", IntegerType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true)),
          propertyType("address", StringType(isNullable = false))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[:`LIVES_IN` => {`address` :: STRING NOT NULL, `since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`address` :: STRING NOT NULL, `since` :: DATE}]->(:`City` =>)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EEP-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (p)-[:KNOWS => {since :: DATE}]->(p) }""".stripMargin,
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true)),
          propertyType("age", IntegerType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "KNOWS",
          nodeTypeRefByVar("p"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[:`KNOWS` => {`since` :: DATE}]->(`p`)
        |}""".stripMargin,
      """{
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`KNOWS` => {`since` :: DATE}]->(:`Person` =>)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EPANY-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (p)-[:KNOWS => {since :: DATE}]->() }""",
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true)),
          propertyType("age", IntegerType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "KNOWS",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[:`KNOWS` => {`since` :: DATE}]->()
        |}""".stripMargin,
      """{
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`KNOWS` => {`since` :: DATE}]->()
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EPANY-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (c :City => {name :: STRING, population :: INT}), ()-[:LIVES_IN =>]->(c) }""",
      graphType(
        nodeType(
          "City",
          "c",
          propertyType("name", StringType(isNullable = true)),
          propertyType("population", IntegerType(isNullable = true))
        ),
        edgeType(EmptyNodeTypeReference()(defaultPos), "LIVES_IN", nodeTypeRefByVar("c"))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  ()-[:`LIVES_IN` =>]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  ()-[:`LIVES_IN` =>]->(:`City` =>)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EPANY-3-1",
      """ALTER CURRENT GRAPH TYPE SET { ()-[:RELATED => {reason :: STRING}]->() }""",
      graphType(
        edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true))
        )
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->()
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->()
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EPANY-4-1",
      """ALTER CURRENT GRAPH TYPE SET { ()-[:RELATED => {reason :: STRING, since :: DATE}]->() }""",
      graphType(
        edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true)),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->()
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->()
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EPANY-4-2",
      """ALTER CURRENT GRAPH TYPE SET { ()-[:RELATED => {since :: DATE, reason :: STRING}]->() }""",
      graphType(
        edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true)),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->()
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->()
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EPANT-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (:City => :Location {name :: STRING}),
        |  (:Village => :Location {name :: STRING}),
        |  (p)-[:KNOWS => {since :: DATE}]->(:Location) }""".stripMargin,
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true)),
          propertyType("age", IntegerType(isNullable = true))
        ),
        nodeType(
          "City",
          Set("Location"),
          propertyType("name", StringType(isNullable = true))
        ),
        nodeType(
          "Village",
          Set("Location"),
          propertyType("name", StringType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "KNOWS",
          nodeTypeRefByLabel("Location"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (:`City` => :`Location` {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Village` => :`Location` {`name` :: STRING}),
        |  (`p`)-[:`KNOWS` => {`since` :: DATE}]->(:`Location`)
        |}""".stripMargin,
      """{
        |  (:`City` => :`Location` {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Village` => :`Location` {`name` :: STRING}),
        |  (:`Person` =>)-[:`KNOWS` => {`since` :: DATE}]->(:`Location`)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EPANT-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (:City => :Location {name :: STRING}),
        |  (:Village => :Location {name :: STRING}),
        |  (:Location)-[:NEAR_BY =>]->(:Location) }""".stripMargin,
      graphType(
        nodeType(
          "City",
          Set("Location"),
          propertyType("name", StringType(isNullable = true))
        ),
        nodeType(
          "Village",
          Set("Location"),
          propertyType("name", StringType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByLabel("Location"),
          "NEAR_BY",
          nodeTypeRefByLabel("Location")
        )
      ),
      """{
        |  (:`City` => :`Location` {`name` :: STRING}),
        |  (:`Village` => :`Location` {`name` :: STRING}),
        |  (:`Location`)-[:`NEAR_BY` =>]->(:`Location`)
        |}""".stripMargin,
      """{
        |  (:`City` => :`Location` {`name` :: STRING}),
        |  (:`Village` => :`Location` {`name` :: STRING}),
        |  (:`Location`)-[:`NEAR_BY` =>]->(:`Location`)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EPANT-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (:City => :Location {name :: STRING}),
        |  (:Village => :Location {name :: STRING}), ()-[:NEAR_BY =>]->(:Location) }""".stripMargin,
      graphType(
        nodeType(
          "City",
          Set("Location"),
          propertyType("name", StringType(isNullable = true))
        ),
        nodeType(
          "Village",
          Set("Location"),
          propertyType("name", StringType(isNullable = true))
        ),
        edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "NEAR_BY",
          nodeTypeRefByLabel("Location")
        )
      ),
      """{
        |  (:`City` => :`Location` {`name` :: STRING}),
        |  (:`Village` => :`Location` {`name` :: STRING}),
        |  ()-[:`NEAR_BY` =>]->(:`Location`)
        |}""".stripMargin,
      """{
        |  (:`City` => :`Location` {`name` :: STRING}),
        |  (:`Village` => :`Location` {`name` :: STRING}),
        |  ()-[:`NEAR_BY` =>]->(:`Location`)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-SPVT-1-1",
      """ALTER CURRENT GRAPH TYPE SET { ()-[:LIVES_IN => {data :: ANY NOT NULL}]->() }""",
      graphType(
        edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "LIVES_IN",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("data", AnyType(isNullable = false))
        )
      ),
      """{
        |  ()-[:`LIVES_IN` => {`data` :: ANY NOT NULL}]->()
        |}""".stripMargin,
      """{
        |  ()-[:`LIVES_IN` => {`data` :: ANY NOT NULL}]->()
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-SPVT-2-1",
      """ALTER CURRENT GRAPH TYPE SET { ()-[:LIVES_IN => {since :: DATE NOT NULL, rating :: INT|STRING }]->() }""",
      graphType(
        edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "LIVES_IN",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("since", DateType(isNullable = false)),
          propertyType(
            "rating",
            ClosedDynamicUnionType(Set(
              IntegerType(isNullable = true)(defaultPos),
              StringType(isNullable = true)(defaultPos)
            ))
          )
        )
      ),
      """{
        |  ()-[:`LIVES_IN` => {`rating` :: STRING | INTEGER, `since` :: DATE NOT NULL}]->()
        |}""".stripMargin,
      """{
        |  ()-[:`LIVES_IN` => {`rating` :: STRING | INTEGER, `since` :: DATE NOT NULL}]->()
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-SPVT-2-2",
      """ALTER CURRENT GRAPH TYPE SET { ()-[:LIVES_IN => {since :: DATE NOT NULL, rating :: ANY<INT|STRING>}]->() }""",
      graphType(
        edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "LIVES_IN",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("since", DateType(isNullable = false)),
          propertyType(
            "rating",
            ClosedDynamicUnionType(Set(
              IntegerType(isNullable = true)(defaultPos),
              StringType(isNullable = true)(defaultPos)
            ))
          )
        )
      ),
      """{
        |  ()-[:`LIVES_IN` => {`rating` :: STRING | INTEGER, `since` :: DATE NOT NULL}]->()
        |}""".stripMargin,
      """{
        |  ()-[:`LIVES_IN` => {`rating` :: STRING | INTEGER, `since` :: DATE NOT NULL}]->()
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-SPVT-2-3",
      """ALTER CURRENT GRAPH TYPE SET { ()-[:LIVES_IN => {since :: DATE NOT NULL, rating :: ANY<STRING|INT>}]->() }""",
      graphType(
        edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "LIVES_IN",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("since", DateType(isNullable = false)),
          propertyType(
            "rating",
            ClosedDynamicUnionType(Set(
              StringType(isNullable = true)(defaultPos),
              IntegerType(isNullable = true)(defaultPos)
            ))
          )
        )
      ),
      """{
        |  ()-[:`LIVES_IN` => {`rating` :: STRING | INTEGER, `since` :: DATE NOT NULL}]->()
        |}""".stripMargin,
      """{
        |  ()-[:`LIVES_IN` => {`rating` :: STRING | INTEGER, `since` :: DATE NOT NULL}]->()
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-SPVT-3-1",
      """ALTER CURRENT GRAPH TYPE SET { ()-[:LIVES_IN => {addresses :: LIST<STRING NOT NULL>}]->() }""",
      graphType(
        edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "LIVES_IN",
          EmptyNodeTypeReference()(defaultPos),
          propertyType(
            "addresses",
            ListType(
              StringType(isNullable = false)(defaultPos),
              isNullable = true
            )
          )
        )
      ),
      """{
        |  ()-[:`LIVES_IN` => {`addresses` :: LIST<STRING NOT NULL>}]->()
        |}""".stripMargin,
      """{
        |  ()-[:`LIVES_IN` => {`addresses` :: LIST<STRING NOT NULL>}]->()
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKC-1-1",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING IS KEY}]->() }""",
      graphType(
        edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos))
        )
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING IS KEY}]->()
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKC-1-2",
      """ALTER CURRENT GRAPH TYPE SET { ()-[r :RELATED => {reason :: STRING}]->() REQUIRE r.reason IS KEY }""",
      graphType(
        edgeTypeWithConstraints(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          "r",
          EmptyNodeTypeReference()(defaultPos),
          Set(KeyConstraint(ArraySeq(prop("r", "reason")))(defaultPos)),
          propertyType("reason", StringType(isNullable = true))
        )
      ),
      """{
        |  ()-[`r`:`RELATED` => {`reason` :: STRING}]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKC-1-3",
      """ALTER CURRENT GRAPH TYPE SET { ()-[r :RELATED => {reason :: STRING}]->(),
        |  CONSTRAINT FOR ()-[r]->() REQUIRE r.reason IS KEY }""".stripMargin,
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          "r",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true))
        )),
        List(keyConstraint(edgeTypeRefByVar("r"), ArraySeq(prop("r", "reason"))))
      ),
      """{
        |  ()-[`r`:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`r`]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKC-1-4",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING}]->(), CONSTRAINT FOR ()-[r :RELATED]->() REQUIRE r.reason IS KEY }""",
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true))
        )),
        List(keyConstraint(edgeTypeRefByLabel("RELATED", "r"), ArraySeq(prop("r", "reason"))))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED`]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKC-1-5",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING}]->(), CONSTRAINT FOR ()-[r :RELATED =>]->() REQUIRE r.reason IS KEY }""",
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true))
        )),
        List(keyConstraint(identifyingEdgeTypeRef("RELATED", "r"), ArraySeq(prop("r", "reason"))))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKC-1-6",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING}]->(), CONSTRAINT FOR ()-[bar :RELATED]->() REQUIRE bar.reason IS KEY }""",
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true))
        )),
        List(keyConstraint(edgeTypeRefByLabel("RELATED", "bar"), ArraySeq(prop("bar", "reason"))))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`bar`:`RELATED`]->() REQUIRE (`bar`.`reason`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKC-1-7",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING}]->(), CONSTRAINT FOR ()-[bar :RELATED =>]->() REQUIRE bar.reason IS KEY }""",
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true))
        )),
        List(keyConstraint(identifyingEdgeTypeRef("RELATED", "bar"), ArraySeq(prop("bar", "reason"))))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`bar`:`RELATED` =>]->() REQUIRE (`bar`.`reason`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKC-2-1",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING NOT NULL IS KEY}]->() }""",
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = false), PropertyInlineKeyConstraint()(defaultPos))
        )),
        List()
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL IS KEY}]->()
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKC-2-2",
      """ALTER CURRENT GRAPH TYPE SET { ()-[r :RELATED => {reason :: STRING NOT NULL}]->() REQUIRE r.reason IS KEY }""",
      graphType(
        List(edgeTypeWithConstraints(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          "r",
          EmptyNodeTypeReference()(defaultPos),
          Set(KeyConstraint(ArraySeq(prop("r", "reason")))(defaultPos)),
          propertyType("reason", StringType(isNullable = false))
        )),
        List()
      ),
      """{
        |  ()-[`r`:`RELATED` => {`reason` :: STRING NOT NULL}]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKC-2-3",
      """ALTER CURRENT GRAPH TYPE SET { ()-[r :RELATED => {reason :: STRING NOT NULL}]->(),  CONSTRAINT FOR ()-[r]->() REQUIRE r.reason IS KEY }""",
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          "r",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = false))
        )),
        List(keyConstraint(edgeTypeRefByVar("r"), ArraySeq(prop("r", "reason"))))
      ),
      """{
        |  ()-[`r`:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`r`]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKC-2-4",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING NOT NULL}]->(), CONSTRAINT FOR ()-[r :RELATED]->() REQUIRE r.reason IS KEY }""",
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = false))
        )),
        List(keyConstraint(edgeTypeRefByLabel("RELATED", "r"), ArraySeq(prop("r", "reason"))))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED`]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKC-2-5",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING NOT NULL}]->(), CONSTRAINT FOR ()-[r :RELATED =>]->() REQUIRE r.reason IS KEY }""",
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = false))
        )),
        List(keyConstraint(identifyingEdgeTypeRef("RELATED", "r"), ArraySeq(prop("r", "reason"))))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKC-2-6",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING NOT NULL}]->(), CONSTRAINT FOR ()-[bar :RELATED]->() REQUIRE bar.reason IS KEY }""",
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = false))
        )),
        List(keyConstraint(edgeTypeRefByLabel("RELATED", "bar"), ArraySeq(prop("bar", "reason"))))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`bar`:`RELATED`]->() REQUIRE (`bar`.`reason`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKC-2-7",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING NOT NULL}]->(), CONSTRAINT FOR ()-[bar :RELATED =>]->() REQUIRE bar.reason IS KEY }""",
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = false))
        )),
        List(keyConstraint(identifyingEdgeTypeRef("RELATED", "bar"), ArraySeq(prop("bar", "reason"))))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`bar`:`RELATED` =>]->() REQUIRE (`bar`.`reason`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKC-3-1",
      """ALTER CURRENT GRAPH TYPE SET { ()-[r :RELATED => {reason :: STRING, since :: DATE}]->() REQUIRE (r.reason, r.since) IS KEY }""",
      graphType(
        List(edgeTypeWithConstraints(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          "r",
          EmptyNodeTypeReference()(defaultPos),
          Set(KeyConstraint(ArraySeq(prop("r", "reason"), prop("r", "since")))(defaultPos)),
          propertyType("reason", StringType(isNullable = true)),
          propertyType("since", DateType(isNullable = true))
        )),
        List()
      ),
      """{
        |  ()-[`r`:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->() REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKC-3-2",
      """ALTER CURRENT GRAPH TYPE SET { ()-[r :RELATED => {reason :: STRING, since :: DATE}]->(), CONSTRAINT FOR ()-[r]->() REQUIRE (r.reason, r.since) IS KEY }""",
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          "r",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true)),
          propertyType("since", DateType(isNullable = true))
        )),
        List(keyConstraint(edgeTypeRefByVar("r"), ArraySeq(prop("r", "reason"), prop("r", "since"))))
      ),
      """{
        |  ()-[`r`:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`]->() REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKC-3-3",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING, since :: DATE}]->(), CONSTRAINT FOR ()-[r :RELATED]->() REQUIRE (r.reason, r.since) IS KEY }""",
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true)),
          propertyType("since", DateType(isNullable = true))
        )),
        List(keyConstraint(edgeTypeRefByLabel("RELATED", "r"), ArraySeq(prop("r", "reason"), prop("r", "since"))))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED`]->() REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKC-3-4",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING, since :: DATE}]->(),
        |  CONSTRAINT FOR ()-[r :RELATED =>]->() REQUIRE (r.reason, r.since) IS KEY }""".stripMargin,
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true)),
          propertyType("since", DateType(isNullable = true))
        )),
        List(keyConstraint(
          identifyingEdgeTypeRef("RELATED", "r"),
          ArraySeq(prop("r", "reason"), prop("r", "since"))
        ))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKC-3-5",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING, since :: DATE}]->(),
        |  CONSTRAINT FOR ()-[bar :RELATED]->() REQUIRE (bar.reason, bar.since) IS KEY }""".stripMargin,
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true)),
          propertyType("since", DateType(isNullable = true))
        )),
        List(keyConstraint(
          edgeTypeRefByLabel("RELATED", "bar"),
          ArraySeq(prop("bar", "reason"), prop("bar", "since"))
        ))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`bar`:`RELATED`]->() REQUIRE (`bar`.`reason`, `bar`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKC-3-6",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING, since :: DATE}]->(),
        |  CONSTRAINT FOR ()-[bar :RELATED =>]->() REQUIRE (bar.reason, bar.since) IS KEY }""".stripMargin,
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true)),
          propertyType("since", DateType(isNullable = true))
        )),
        List(keyConstraint(
          identifyingEdgeTypeRef("RELATED", "bar"),
          ArraySeq(prop("bar", "reason"), prop("bar", "since"))
        ))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`bar`:`RELATED` =>]->() REQUIRE (`bar`.`reason`, `bar`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EUC-1-1",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING IS UNIQUE}]->() }""",
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true), PropertyInlineUniquenessConstraint()(defaultPos))
        )),
        List()
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING IS UNIQUE}]->()
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EUC-1-2",
      """ALTER CURRENT GRAPH TYPE SET { ()-[r :RELATED => {reason :: STRING}]->() REQUIRE r.reason IS UNIQUE }""".stripMargin,
      graphType(
        List(edgeTypeWithConstraints(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          "r",
          EmptyNodeTypeReference()(defaultPos),
          Set(UniquenessConstraint(ArraySeq(prop("r", "reason")))(defaultPos)),
          propertyType("reason", StringType(isNullable = true))
        )),
        List()
      ),
      """{
        |  ()-[`r`:`RELATED` => {`reason` :: STRING}]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EUC-1-3",
      """ALTER CURRENT GRAPH TYPE SET { ()-[r :RELATED => {reason :: STRING}]->(),
        |  CONSTRAINT FOR ()-[r]->() REQUIRE r.reason IS UNIQUE }""".stripMargin,
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          "r",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true))
        )),
        List(uniquenessConstraint(edgeTypeRefByVar("r"), ArraySeq(prop("r", "reason"))))
      ),
      """{
        |  ()-[`r`:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`r`]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EUC-1-4",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING}]->(),
        |  CONSTRAINT FOR ()-[r :RELATED]->() REQUIRE r.reason IS UNIQUE }""".stripMargin,
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true))
        )),
        List(uniquenessConstraint(edgeTypeRefByLabel("RELATED", "r"), ArraySeq(prop("r", "reason"))))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED`]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EUC-1-5",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING}]->(),
        |  CONSTRAINT FOR ()-[r :RELATED =>]->() REQUIRE r.reason IS UNIQUE }""".stripMargin,
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true))
        )),
        List(uniquenessConstraint(identifyingEdgeTypeRef("RELATED", "r"), ArraySeq(prop("r", "reason"))))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EUC-1-6",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING}]->(),
        |  CONSTRAINT FOR ()-[bar :RELATED]->() REQUIRE bar.reason IS UNIQUE }""".stripMargin,
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true))
        )),
        List(uniquenessConstraint(edgeTypeRefByLabel("RELATED", "bar"), ArraySeq(prop("bar", "reason"))))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`bar`:`RELATED`]->() REQUIRE (`bar`.`reason`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EUC-1-7",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING}]->(),
        |  CONSTRAINT FOR ()-[bar :RELATED =>]->() REQUIRE bar.reason IS UNIQUE }""".stripMargin,
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true))
        )),
        List(uniquenessConstraint(identifyingEdgeTypeRef("RELATED", "bar"), ArraySeq(prop("bar", "reason"))))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`bar`:`RELATED` =>]->() REQUIRE (`bar`.`reason`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EUC-2-1",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING NOT NULL IS UNIQUE}]->() }""",
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = false), PropertyInlineUniquenessConstraint()(defaultPos))
        )),
        List()
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL IS UNIQUE}]->()
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EUC-2-2",
      """ALTER CURRENT GRAPH TYPE SET { ()-[r :RELATED => {reason :: STRING NOT NULL}]->() REQUIRE r.reason IS UNIQUE }""",
      graphType(
        List(edgeTypeWithConstraints(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          "r",
          EmptyNodeTypeReference()(defaultPos),
          Set(UniquenessConstraint(ArraySeq(prop("r", "reason")))(defaultPos)),
          propertyType("reason", StringType(isNullable = false))
        )),
        List()
      ),
      """{
        |  ()-[`r`:`RELATED` => {`reason` :: STRING NOT NULL}]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EUC-2-3",
      """ALTER CURRENT GRAPH TYPE SET { ()-[r :RELATED => {reason :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[r]->() REQUIRE r.reason IS UNIQUE }""".stripMargin,
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          "r",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = false))
        )),
        List(uniquenessConstraint(edgeTypeRefByVar("r"), ArraySeq(prop("r", "reason"))))
      ),
      """{
        |  ()-[`r`:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`r`]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EUC-2-4",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[r :RELATED]->() REQUIRE r.reason IS UNIQUE }""".stripMargin,
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = false))
        )),
        List(uniquenessConstraint(edgeTypeRefByLabel("RELATED", "r"), ArraySeq(prop("r", "reason"))))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED`]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EUC-2-5",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[r :RELATED =>]->() REQUIRE r.reason IS UNIQUE }""".stripMargin,
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = false))
        )),
        List(uniquenessConstraint(identifyingEdgeTypeRef("RELATED", "r"), ArraySeq(prop("r", "reason"))))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EUC-2-6",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[bar :RELATED]->() REQUIRE bar.reason IS UNIQUE }""".stripMargin,
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = false))
        )),
        List(uniquenessConstraint(edgeTypeRefByLabel("RELATED", "bar"), ArraySeq(prop("bar", "reason"))))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`bar`:`RELATED`]->() REQUIRE (`bar`.`reason`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EUC-2-7",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[bar :RELATED =>]->() REQUIRE bar.reason IS UNIQUE }""".stripMargin,
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = false))
        )),
        List(uniquenessConstraint(identifyingEdgeTypeRef("RELATED", "bar"), ArraySeq(prop("bar", "reason"))))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`bar`:`RELATED` =>]->() REQUIRE (`bar`.`reason`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING NOT NULL}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EUC-3-1",
      """ALTER CURRENT GRAPH TYPE SET { ()-[r :RELATED => {reason :: STRING, since :: DATE}]->() REQUIRE (r.reason, r.since) IS UNIQUE }""",
      graphType(
        List(edgeTypeWithConstraints(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          "r",
          EmptyNodeTypeReference()(defaultPos),
          Set(UniquenessConstraint(ArraySeq(prop("r", "reason"), prop("r", "since")))(defaultPos)),
          propertyType("reason", StringType(isNullable = true)),
          propertyType("since", DateType(isNullable = true))
        )),
        List()
      ),
      """{
        |  ()-[`r`:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->() REQUIRE (`r`.`reason`, `r`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EUC-3-2",
      """ALTER CURRENT GRAPH TYPE SET { ()-[r :RELATED => {reason :: STRING, since :: DATE}]->(), CONSTRAINT FOR ()-[r]->() REQUIRE (r.reason, r.since) IS UNIQUE }""",
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          "r",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true)),
          propertyType("since", DateType(isNullable = true))
        )),
        List(uniquenessConstraint(edgeTypeRefByVar("r"), ArraySeq(prop("r", "reason"), prop("r", "since"))))
      ),
      """{
        |  ()-[`r`:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`]->() REQUIRE (`r`.`reason`, `r`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EUC-3-3",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING, since :: DATE}]->(),
        |  CONSTRAINT FOR ()-[r :RELATED]->() REQUIRE (r.reason, r.since) IS UNIQUE }""".stripMargin,
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true)),
          propertyType("since", DateType(isNullable = true))
        )),
        List(uniquenessConstraint(
          edgeTypeRefByLabel("RELATED", "r"),
          ArraySeq(prop("r", "reason"), prop("r", "since"))
        ))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED`]->() REQUIRE (`r`.`reason`, `r`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EUC-3-4",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING, since :: DATE}]->(),
        |  CONSTRAINT FOR ()-[r :RELATED =>]->() REQUIRE (r.reason, r.since) IS UNIQUE }""".stripMargin,
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true)),
          propertyType("since", DateType(isNullable = true))
        )),
        List(uniquenessConstraint(
          identifyingEdgeTypeRef("RELATED", "r"),
          ArraySeq(prop("r", "reason"), prop("r", "since"))
        ))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EUC-3-5",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING, since :: DATE}]->(),
        |  CONSTRAINT FOR ()-[bar :RELATED]->() REQUIRE (bar.reason, bar.since) IS UNIQUE }""".stripMargin,
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true)),
          propertyType("since", DateType(isNullable = true))
        )),
        List(uniquenessConstraint(
          edgeTypeRefByLabel("RELATED", "bar"),
          ArraySeq(prop("bar", "reason"), prop("bar", "since"))
        ))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`bar`:`RELATED`]->() REQUIRE (`bar`.`reason`, `bar`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EUC-3-6",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING, since :: DATE}]->(),
        |  CONSTRAINT FOR ()-[bar :RELATED =>]->() REQUIRE (bar.reason, bar.since) IS UNIQUE }""".stripMargin,
      graphType(
        List(edgeType(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("reason", StringType(isNullable = true)),
          propertyType("since", DateType(isNullable = true))
        )),
        List(uniquenessConstraint(
          identifyingEdgeTypeRef("RELATED", "bar"),
          ArraySeq(prop("bar", "reason"), prop("bar", "since"))
        ))
      ),
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`bar`:`RELATED` =>]->() REQUIRE (`bar`.`reason`, `bar`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKUC-1-1",
      """ALTER CURRENT GRAPH TYPE SET { ()-[r :RELATED => {reason :: STRING, since :: DATE, rating :: INT}]->()
        |  REQUIRE (r.reason, r.since) IS KEY REQUIRE (r.rating) IS KEY }""".stripMargin,
      graphType(
        List(edgeTypeWithConstraints(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          "r",
          EmptyNodeTypeReference()(defaultPos),
          Set(
            KeyConstraint(ArraySeq(prop("r", "reason"), prop("r", "since")))(defaultPos),
            KeyConstraint(ArraySeq(prop("r", "rating")))(defaultPos)
          ),
          propertyType("reason", StringType(isNullable = true)),
          propertyType("since", DateType(isNullable = true)),
          propertyType("rating", IntegerType(isNullable = true))
        )),
        List()
      ),
      """{
        |  ()-[`r`:`RELATED` => {`rating` :: INTEGER, `reason` :: STRING, `since` :: DATE}]->() REQUIRE (`r`.`rating`) IS KEY REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`rating` :: INTEGER, `reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`rating`) IS KEY,
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKUC-2-1",
      """ALTER CURRENT GRAPH TYPE SET { ()-[r :RELATED => {reason :: STRING, since :: DATE, rating :: INT}]->()
        |  REQUIRE (r.reason, r.since) IS UNIQUE REQUIRE (r.rating) IS UNIQUE }""".stripMargin,
      graphType(
        List(edgeTypeWithConstraints(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          "r",
          EmptyNodeTypeReference()(defaultPos),
          Set(
            UniquenessConstraint(ArraySeq(prop("r", "reason"), prop("r", "since")))(defaultPos),
            UniquenessConstraint(ArraySeq(prop("r", "rating")))(defaultPos)
          ),
          propertyType("reason", StringType(isNullable = true)),
          propertyType("since", DateType(isNullable = true)),
          propertyType("rating", IntegerType(isNullable = true))
        )),
        List()
      ),
      """{
        |  ()-[`r`:`RELATED` => {`rating` :: INTEGER, `reason` :: STRING, `since` :: DATE}]->() REQUIRE (`r`.`rating`) IS UNIQUE REQUIRE (`r`.`reason`, `r`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`rating` :: INTEGER, `reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`rating`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKUC-3-1",
      """ALTER CURRENT GRAPH TYPE SET { ()-[r :RELATED => {reason :: STRING, since :: DATE, rating :: INT}]->()
        |  REQUIRE (r.reason, r.since) IS KEY REQUIRE (r.rating) IS UNIQUE }""".stripMargin,
      graphType(
        List(edgeTypeWithConstraints(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          "r",
          EmptyNodeTypeReference()(defaultPos),
          Set(
            KeyConstraint(ArraySeq(prop("r", "reason"), prop("r", "since")))(defaultPos),
            UniquenessConstraint(ArraySeq(prop("r", "rating")))(defaultPos)
          ),
          propertyType("reason", StringType(isNullable = true)),
          propertyType("since", DateType(isNullable = true)),
          propertyType("rating", IntegerType(isNullable = true))
        )),
        List()
      ),
      """{
        |  ()-[`r`:`RELATED` => {`rating` :: INTEGER, `reason` :: STRING, `since` :: DATE}]->() REQUIRE (`r`.`rating`) IS UNIQUE REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`rating` :: INTEGER, `reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`rating`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKUC-3-2",
      """ALTER CURRENT GRAPH TYPE SET { ()-[r :RELATED => {reason :: STRING, since :: DATE, rating :: INT}]->()
        |  REQUIRE (r.rating) IS UNIQUE REQUIRE (r.reason, r.since) IS KEY }""".stripMargin,
      graphType(
        List(edgeTypeWithConstraints(
          EmptyNodeTypeReference()(defaultPos),
          "RELATED",
          "r",
          EmptyNodeTypeReference()(defaultPos),
          Set(
            KeyConstraint(ArraySeq(prop("r", "reason"), prop("r", "since")))(defaultPos),
            UniquenessConstraint(ArraySeq(prop("r", "rating")))(defaultPos)
          ),
          propertyType("reason", StringType(isNullable = true)),
          propertyType("since", DateType(isNullable = true)),
          propertyType("rating", IntegerType(isNullable = true))
        )),
        List()
      ),
      """{
        |  ()-[`r`:`RELATED` => {`rating` :: INTEGER, `reason` :: STRING, `since` :: DATE}]->() REQUIRE (`r`.`rating`) IS UNIQUE REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`rating` :: INTEGER, `reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`rating`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKUC-3-3",
      """ALTER CURRENT GRAPH TYPE SET { ()-[r :RELATED => {reason :: STRING, since :: DATE, rating :: INT}]->(),
        |  CONSTRAINT FOR ()-[r]->() REQUIRE (r.reason, r.since) IS KEY,
        |  CONSTRAINT FOR ()-[r]->() REQUIRE (r.rating) IS UNIQUE }""".stripMargin,
      graphType(
        Seq(
          edgeType(
            EmptyNodeTypeReference()(defaultPos),
            "RELATED",
            "r",
            EmptyNodeTypeReference()(defaultPos),
            propertyType("since", DateType(isNullable = true)),
            propertyType("reason", StringType(isNullable = true)),
            propertyType("rating", IntegerType(isNullable = true))
          )
        ),
        Seq(
          keyConstraint(
            edgeTypeRefByVar("r"),
            ArraySeq(prop("r", "reason"), prop("r", "since"))
          ),
          uniquenessConstraint(edgeTypeRefByVar("r"), ArraySeq(prop("r", "rating")))
        )
      ),
      """{
        |  ()-[`r`:`RELATED` => {`rating` :: INTEGER, `reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`]->() REQUIRE (`r`.`rating`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`]->() REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`rating` :: INTEGER, `reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`rating`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKUC-3-4",
      """ALTER CURRENT GRAPH TYPE SET { ()-[  :RELATED => {reason :: STRING, since :: DATE, rating :: INT}]->(),
        |  CONSTRAINT FOR ()-[r :RELATED]->() REQUIRE (r.reason, r.since) IS KEY,
        |  CONSTRAINT FOR ()-[x :RELATED]->() REQUIRE x.rating IS UNIQUE }""".stripMargin,
      graphType(
        Seq(
          edgeType(
            EmptyNodeTypeReference()(defaultPos),
            "RELATED",
            EmptyNodeTypeReference()(defaultPos),
            propertyType("since", DateType(isNullable = true)),
            propertyType("reason", StringType(isNullable = true)),
            propertyType("rating", IntegerType(isNullable = true))
          )
        ),
        Seq(
          keyConstraint(
            edgeTypeRefByLabel("RELATED", "r"),
            ArraySeq(prop("r", "reason"), prop("r", "since"))
          ),
          uniquenessConstraint(edgeTypeRefByLabel("RELATED", "x"), ArraySeq(prop("x", "rating")))
        )
      ),
      """{
        |  ()-[:`RELATED` => {`rating` :: INTEGER, `reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`x`:`RELATED`]->() REQUIRE (`x`.`rating`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`:`RELATED`]->() REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`rating` :: INTEGER, `reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`rating`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKUC-3-5",
      """ALTER CURRENT GRAPH TYPE SET { ()-[r :RELATED => {reason :: STRING, since :: DATE, rating :: INT}]->(),
        |  CONSTRAINT FOR ()-[bar :RELATED]->() REQUIRE (bar.reason, bar.since) IS KEY,
        |  CONSTRAINT FOR ()-[x :RELATED]->() REQUIRE x.rating IS UNIQUE }""".stripMargin,
      graphType(
        Seq(
          edgeType(
            EmptyNodeTypeReference()(defaultPos),
            "RELATED",
            "r",
            EmptyNodeTypeReference()(defaultPos),
            propertyType("since", DateType(isNullable = true)),
            propertyType("reason", StringType(isNullable = true)),
            propertyType("rating", IntegerType(isNullable = true))
          )
        ),
        Seq(
          keyConstraint(
            edgeTypeRefByLabel("RELATED", "bar"),
            ArraySeq(prop("bar", "reason"), prop("bar", "since"))
          ),
          uniquenessConstraint(edgeTypeRefByLabel("RELATED", "x"), ArraySeq(prop("x", "rating")))
        )
      ),
      """{
        |  ()-[`r`:`RELATED` => {`rating` :: INTEGER, `reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`x`:`RELATED`]->() REQUIRE (`x`.`rating`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`bar`:`RELATED`]->() REQUIRE (`bar`.`reason`, `bar`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`rating` :: INTEGER, `reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`rating`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKUC-3-6",
      """ALTER CURRENT GRAPH TYPE SET { ()-[r :RELATED => {since :: DATE, reason :: STRING, rating :: INT}]->(),
        |  CONSTRAINT FOR ()-[x :RELATED]->() REQUIRE x.rating IS UNIQUE,
        |  CONSTRAINT FOR ()-[bar :RELATED]->() REQUIRE (bar.reason, bar.since) IS KEY }""".stripMargin,
      graphType(
        Seq(
          edgeType(
            EmptyNodeTypeReference()(defaultPos),
            "RELATED",
            "r",
            EmptyNodeTypeReference()(defaultPos),
            propertyType("since", DateType(isNullable = true)),
            propertyType("reason", StringType(isNullable = true)),
            propertyType("rating", IntegerType(isNullable = true))
          )
        ),
        Seq(
          uniquenessConstraint(edgeTypeRefByLabel("RELATED", "x"), ArraySeq(prop("x", "rating"))),
          keyConstraint(
            edgeTypeRefByLabel("RELATED", "bar"),
            ArraySeq(prop("bar", "reason"), prop("bar", "since"))
          )
        )
      ),
      """{
        |  ()-[`r`:`RELATED` => {`rating` :: INTEGER, `reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`x`:`RELATED`]->() REQUIRE (`x`.`rating`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`bar`:`RELATED`]->() REQUIRE (`bar`.`reason`, `bar`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`rating` :: INTEGER, `reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`rating`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "SET-PE-EKUC-3-7",
      """ALTER CURRENT GRAPH TYPE SET { ()-[r :RELATED => {since :: DATE, reason :: STRING, rating :: INT}]->(),
        |  CONSTRAINT FOR ()-[x :RELATED =>]->() REQUIRE x.rating IS UNIQUE,
        |  CONSTRAINT FOR ()-[bar :RELATED =>]->() REQUIRE (bar.reason, bar.since) IS KEY }""".stripMargin,
      graphType(
        Seq(
          edgeType(
            EmptyNodeTypeReference()(defaultPos),
            "RELATED",
            "r",
            EmptyNodeTypeReference()(defaultPos),
            propertyType("since", DateType(isNullable = true)),
            propertyType("reason", StringType(isNullable = true)),
            propertyType("rating", IntegerType(isNullable = true))
          )
        ),
        Seq(
          uniquenessConstraint(identifyingEdgeTypeRef("RELATED", "x"), ArraySeq(prop("x", "rating"))),
          keyConstraint(
            identifyingEdgeTypeRef("RELATED", "bar"),
            ArraySeq(prop("bar", "reason"), prop("bar", "since"))
          )
        )
      ),
      """{
        |  ()-[`r`:`RELATED` => {`rating` :: INTEGER, `reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`x`:`RELATED` =>]->() REQUIRE (`x`.`rating`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`bar`:`RELATED` =>]->() REQUIRE (`bar`.`reason`, `bar`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  ()-[:`RELATED` => {`rating` :: INTEGER, `reason` :: STRING, `since` :: DATE}]->(),
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`rating`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`:`RELATED` =>]->() REQUIRE (`r`.`reason`, `r`.`since`) IS KEY
        |}""".stripMargin
    )
  )

  private def met(): Seq[GraphTypeTestCase] = Seq(
    GraphTypeTestCase(
      "MET-PE-S-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {population :: INT}),
        |  (p)-[:LIVES_IN =>]->(c),
        |  (a :Animal => {family :: STRING}),
        |  (n :Nutriment => {energy :: INT}),
        |  (a)-[:EATS =>]->(n) }""".stripMargin,
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true)),
          propertyType("age", IntegerType(isNullable = true))
        ),
        nodeType(
          "City",
          "c",
          propertyType("population", IntegerType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          nodeTypeRefByVar("c")
        ),
        nodeType(
          "Animal",
          "a",
          propertyType("family", StringType(isNullable = true))
        ),
        nodeType(
          "Nutriment",
          "n",
          propertyType("energy", IntegerType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("a"),
          "EATS",
          nodeTypeRefByVar("n")
        )
      ),
      """{
        |  (`a`:`Animal` => {`family` :: STRING}),
        |  (`c`:`City` => {`population` :: INTEGER}),
        |  (`n`:`Nutriment` => {`energy` :: INTEGER}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`a`)-[:`EATS` =>]->(`n`),
        |  (`p`)-[:`LIVES_IN` =>]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`Animal` => {`family` :: STRING}),
        |  (:`City` => {`population` :: INTEGER}),
        |  (:`Nutriment` => {`energy` :: INTEGER}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Animal` =>)-[:`EATS` =>]->(:`Nutriment` =>),
        |  (:`Person` =>)-[:`LIVES_IN` =>]->(:`City` =>)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MET-PE-CIP-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (m :Dish => {name :: STRING}),
        |  (a :Animal => {family :: STRING}),
        |  (n :Nutriment => {energy :: INT}),
        |  (p)-[:DINES => {frequency :: FLOAT}]->(m),
        |  (a)-[:EATS => {frequency :: FLOAT}]->(n) }""".stripMargin,
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true)),
          propertyType("age", IntegerType(isNullable = true))
        ),
        nodeType(
          "Dish",
          "m",
          propertyType("name", StringType(isNullable = true))
        ),
        nodeType(
          "Animal",
          "a",
          propertyType("family", StringType(isNullable = true))
        ),
        nodeType(
          "Nutriment",
          "n",
          propertyType("energy", IntegerType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "DINES",
          nodeTypeRefByVar("m"),
          propertyType("frequency", FloatType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("a"),
          "EATS",
          nodeTypeRefByVar("n"),
          propertyType("frequency", FloatType(isNullable = true))
        )
      ),
      """{
        |  (`a`:`Animal` => {`family` :: STRING}),
        |  (`m`:`Dish` => {`name` :: STRING}),
        |  (`n`:`Nutriment` => {`energy` :: INTEGER}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[:`DINES` => {`frequency` :: FLOAT}]->(`m`),
        |  (`a`)-[:`EATS` => {`frequency` :: FLOAT}]->(`n`)
        |}""".stripMargin,
      """{
        |  (:`Animal` => {`family` :: STRING}),
        |  (:`Dish` => {`name` :: STRING}),
        |  (:`Nutriment` => {`energy` :: INTEGER}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`DINES` => {`frequency` :: FLOAT}]->(:`Dish` =>),
        |  (:`Animal` =>)-[:`EATS` => {`frequency` :: FLOAT}]->(:`Nutriment` =>)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MET-PE-CIEP-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING, population :: INT}),
        |  (p)-[:LIVES_IN =>]->(c),
        |  (p)-[:KNOWS =>]->(p) }""".stripMargin,
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true)),
          propertyType("age", IntegerType(isNullable = true))
        ),
        nodeType(
          "City",
          "c",
          propertyType("name", StringType(isNullable = true)),
          propertyType("population", IntegerType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          nodeTypeRefByVar("c")
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "KNOWS",
          nodeTypeRefByVar("p")
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[:`KNOWS` =>]->(`p`),
        |  (`p`)-[:`LIVES_IN` =>]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`KNOWS` =>]->(:`Person` =>),
        |  (:`Person` =>)-[:`LIVES_IN` =>]->(:`City` =>)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MET-PE-CIEP-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING, population :: INT}),
        |  (p)-[:LIVES_IN =>]->(c),
        |  (p)-[:KNOWS =>]->(p),
        |  (c)-[:HAS_MAJOR =>]->(p) }""".stripMargin,
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true)),
          propertyType("age", IntegerType(isNullable = true))
        ),
        nodeType(
          "City",
          "c",
          propertyType("name", StringType(isNullable = true)),
          propertyType("population", IntegerType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          nodeTypeRefByVar("c")
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "KNOWS",
          nodeTypeRefByVar("p")
        ),
        edgeType(
          nodeTypeRefByVar("c"),
          "HAS_MAJOR",
          nodeTypeRefByVar("p")
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`c`)-[:`HAS_MAJOR` =>]->(`p`),
        |  (`p`)-[:`KNOWS` =>]->(`p`),
        |  (`p`)-[:`LIVES_IN` =>]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`City` =>)-[:`HAS_MAJOR` =>]->(:`Person` =>),
        |  (:`Person` =>)-[:`KNOWS` =>]->(:`Person` =>),
        |  (:`Person` =>)-[:`LIVES_IN` =>]->(:`City` =>)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MET-PE-CIEP-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (c :City => {name :: STRING, population :: INT}),
        |  (:Person)-[:LIVES_IN =>]->(c),
        |  (:Person)-[:KNOWS =>]->(:Person),
        |  (c)-[:HAS_MAJOR =>]->(:Person) }""".stripMargin,
      graphType(
        nodeType(
          "City",
          "c",
          propertyType("name", StringType(isNullable = true)),
          propertyType("population", IntegerType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByLabel("Person"),
          "LIVES_IN",
          nodeTypeRefByVar("c")
        ),
        edgeType(
          nodeTypeRefByLabel("Person"),
          "KNOWS",
          nodeTypeRefByLabel("Person")
        ),
        edgeType(
          nodeTypeRefByVar("c"),
          "HAS_MAJOR",
          nodeTypeRefByLabel("Person")
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (`c`)-[:`HAS_MAJOR` =>]->(:`Person`),
        |  (:`Person`)-[:`KNOWS` =>]->(:`Person`),
        |  (:`Person`)-[:`LIVES_IN` =>]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (:`City` =>)-[:`HAS_MAJOR` =>]->(:`Person`),
        |  (:`Person`)-[:`KNOWS` =>]->(:`Person`),
        |  (:`Person`)-[:`LIVES_IN` =>]->(:`City` =>)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MET-PE-CIEP-4-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person)-[:LIVES_IN =>]->(:City),
        |  (:Person)-[:KNOWS =>]->(:Person),
        |  (:City)-[:HAS_MAJOR =>]->(:Person) }""".stripMargin,
      graphType(
        edgeType(
          nodeTypeRefByLabel("Person"),
          "LIVES_IN",
          nodeTypeRefByLabel("City")
        ),
        edgeType(
          nodeTypeRefByLabel("Person"),
          "KNOWS",
          nodeTypeRefByLabel("Person")
        ),
        edgeType(
          nodeTypeRefByLabel("City"),
          "HAS_MAJOR",
          nodeTypeRefByLabel("Person")
        )
      ),
      """{
        |  (:`City`)-[:`HAS_MAJOR` =>]->(:`Person`),
        |  (:`Person`)-[:`KNOWS` =>]->(:`Person`),
        |  (:`Person`)-[:`LIVES_IN` =>]->(:`City`)
        |}""".stripMargin,
      """{
        |  (:`City`)-[:`HAS_MAJOR` =>]->(:`Person`),
        |  (:`Person`)-[:`KNOWS` =>]->(:`Person`),
        |  (:`Person`)-[:`LIVES_IN` =>]->(:`City`)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MET-PE-CIEPP-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING, population :: INT}),
        |  (p)-[:LIVES_IN => {since :: DATE}]->(c),
        |  (p)-[:KNOWS => {since :: DATE}]->(p) }""".stripMargin,
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true)),
          propertyType("age", IntegerType(isNullable = true))
        ),
        nodeType(
          "City",
          "c",
          propertyType("name", StringType(isNullable = true)),
          propertyType("population", IntegerType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "KNOWS",
          nodeTypeRefByVar("p"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[:`KNOWS` => {`since` :: DATE}]->(`p`),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`KNOWS` => {`since` :: DATE}]->(:`Person` =>),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MET-PE-CIEPP-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING, population :: INT}),
        |  (p)-[:LIVES_IN =>]->(c),
        |  (p)-[:BORN_IN =>]->(c),
        |  (p)-[:KNOWS =>]->(p) }""".stripMargin,
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true)),
          propertyType("age", IntegerType(isNullable = true))
        ),
        nodeType(
          "City",
          "c",
          propertyType("name", StringType(isNullable = true)),
          propertyType("population", IntegerType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          nodeTypeRefByVar("c")
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "BORN_IN",
          nodeTypeRefByVar("c")
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "KNOWS",
          nodeTypeRefByVar("p")
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[:`BORN_IN` =>]->(`c`),
        |  (`p`)-[:`KNOWS` =>]->(`p`),
        |  (`p`)-[:`LIVES_IN` =>]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING, `population` :: INTEGER}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`BORN_IN` =>]->(:`City` =>),
        |  (:`Person` =>)-[:`KNOWS` =>]->(:`Person` =>),
        |  (:`Person` =>)-[:`LIVES_IN` =>]->(:`City` =>)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MET-PE-CIEPP-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => :Creature {name :: STRING, age :: INT}),
        |  (a :Animal => :Creature {name :: STRING}),
        |  (c :City => :Habitat {name :: STRING, population :: INT}),
        |  (w :Wood => :Habitat {name :: STRING, population :: INT}),
        |  (:Creature)-[:LIVES_IN =>]->(:Habitat),
        |  (p)-[:BORN_IN =>]->(c),
        |  (p)-[:KNOWS => {since :: DATE}]->(),
        |  (w)-[:BELONGS_TO =>]->(c),
        |  (p)-[:IN_LOVE_WITH =>]->(p) }""".stripMargin,
      graphType(
        nodeType(
          "Person",
          "p",
          Set("Creature"),
          propertyType("name", StringType(isNullable = true)),
          propertyType("age", IntegerType(isNullable = true))
        ),
        nodeType(
          "Animal",
          "a",
          Set("Creature"),
          propertyType("name", StringType(isNullable = true))
        ),
        nodeType(
          "City",
          "c",
          Set("Habitat"),
          propertyType("name", StringType(isNullable = true)),
          propertyType("population", IntegerType(isNullable = true))
        ),
        nodeType(
          "Wood",
          "w",
          Set("Habitat"),
          propertyType("name", StringType(isNullable = true)),
          propertyType("population", IntegerType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByLabel("Creature"),
          "LIVES_IN",
          nodeTypeRefByLabel("Habitat")
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "BORN_IN",
          nodeTypeRefByVar("c")
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "KNOWS",
          EmptyNodeTypeReference()(defaultPos),
          propertyType("since", DateType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("w"),
          "BELONGS_TO",
          nodeTypeRefByVar("c")
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "IN_LOVE_WITH",
          nodeTypeRefByVar("p")
        )
      ),
      """{
        |  (`a`:`Animal` => :`Creature` {`name` :: STRING}),
        |  (`c`:`City` => :`Habitat` {`name` :: STRING, `population` :: INTEGER}),
        |  (`p`:`Person` => :`Creature` {`age` :: INTEGER, `name` :: STRING}),
        |  (`w`:`Wood` => :`Habitat` {`name` :: STRING, `population` :: INTEGER}),
        |  (`w`)-[:`BELONGS_TO` =>]->(`c`),
        |  (`p`)-[:`BORN_IN` =>]->(`c`),
        |  (`p`)-[:`IN_LOVE_WITH` =>]->(`p`),
        |  (`p`)-[:`KNOWS` => {`since` :: DATE}]->(),
        |  (:`Creature`)-[:`LIVES_IN` =>]->(:`Habitat`)
        |}""".stripMargin,
      """{
        |  (:`Animal` => :`Creature` {`name` :: STRING}),
        |  (:`City` => :`Habitat` {`name` :: STRING, `population` :: INTEGER}),
        |  (:`Person` => :`Creature` {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Wood` => :`Habitat` {`name` :: STRING, `population` :: INTEGER}),
        |  (:`Wood` =>)-[:`BELONGS_TO` =>]->(:`City` =>),
        |  (:`Person` =>)-[:`BORN_IN` =>]->(:`City` =>),
        |  (:`Person` =>)-[:`IN_LOVE_WITH` =>]->(:`Person` =>),
        |  (:`Person` =>)-[:`KNOWS` => {`since` :: DATE}]->(),
        |  (:`Creature`)-[:`LIVES_IN` =>]->(:`Habitat`)
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MET-PE-KUC-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person)-[:LIVES_IN => {since :: DATE IS KEY}]->(:City),
        |  (:Person)-[:KNOWS => {since :: DATE IS KEY}]->(:Person) }""".stripMargin,
      graphType(
        edgeType(
          nodeTypeRefByLabel("Person"),
          "LIVES_IN",
          nodeTypeRefByLabel("City"),
          propertyType("since", DateType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos))
        ),
        edgeType(
          nodeTypeRefByLabel("Person"),
          "KNOWS",
          nodeTypeRefByLabel("Person"),
          propertyType("since", DateType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos))
        )
      ),
      """{
        |  (:`Person`)-[:`KNOWS` => {`since` :: DATE IS KEY}]->(:`Person`),
        |  (:`Person`)-[:`LIVES_IN` => {`since` :: DATE IS KEY}]->(:`City`)
        |}""".stripMargin,
      """{
        |  (:`Person`)-[:`KNOWS` => {`since` :: DATE}]->(:`Person`),
        |  (:`Person`)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City`),
        |  CONSTRAINT FOR ()-[`r`:`KNOWS` =>]->() REQUIRE (`r`.`since`) IS KEY,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MET-PE-KUC-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person)-[l:LIVES_IN => {street :: STRING, streetNumber :: STRING}]->(:City)
        |  REQUIRE (l.street, l.streetNumber) IS UNIQUE,
        |  (:Person)-[k:KNOWS => {sinceMonth :: INT, sinceYear :: INT}]->(:Person)
        |  REQUIRE (k.sinceMonth, k.sinceYear) IS UNIQUE }""".stripMargin,
      graphType(
        edgeTypeWithConstraints(
          nodeTypeRefByLabel("Person"),
          "LIVES_IN",
          "l",
          nodeTypeRefByLabel("City"),
          Set(UniquenessConstraint(ArraySeq(prop("l", "street"), prop("l", "streetNumber")))(defaultPos)),
          propertyType("street", StringType(isNullable = true)),
          propertyType("streetNumber", StringType(isNullable = true))
        ),
        edgeTypeWithConstraints(
          nodeTypeRefByLabel("Person"),
          "KNOWS",
          "k",
          nodeTypeRefByLabel("Person"),
          Set(UniquenessConstraint(ArraySeq(prop("k", "sinceMonth"), prop("k", "sinceYear")))(defaultPos)),
          propertyType("sinceMonth", IntegerType(isNullable = true)),
          propertyType("sinceYear", IntegerType(isNullable = true))
        )
      ),
      """{
        |  (:`Person`)-[`k`:`KNOWS` => {`sinceMonth` :: INTEGER, `sinceYear` :: INTEGER}]->(:`Person`) REQUIRE (`k`.`sinceMonth`, `k`.`sinceYear`) IS UNIQUE,
        |  (:`Person`)-[`l`:`LIVES_IN` => {`street` :: STRING, `streetNumber` :: STRING}]->(:`City`) REQUIRE (`l`.`street`, `l`.`streetNumber`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person`)-[:`KNOWS` => {`sinceMonth` :: INTEGER, `sinceYear` :: INTEGER}]->(:`Person`),
        |  (:`Person`)-[:`LIVES_IN` => {`street` :: STRING, `streetNumber` :: STRING}]->(:`City`),
        |  CONSTRAINT FOR ()-[`r`:`KNOWS` =>]->() REQUIRE (`r`.`sinceMonth`, `r`.`sinceYear`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`street`, `r`.`streetNumber`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MET-PE-KUC-2-2",
      """ALTER CURRENT GRAPH TYPE SET { (:Person)-[:LIVES_IN => {street :: STRING, streetNumber :: STRING}]->(:City),
        |  (:Person)-[:KNOWS => {sinceMonth :: INT, sinceYear :: INT}]->(:Person),
        |  CONSTRAINT FOR ()-[x:LIVES_IN =>]->() REQUIRE (x.street, x.streetNumber) IS UNIQUE,
        |  CONSTRAINT FOR ()-[x:KNOWS =>]->() REQUIRE (x.sinceMonth, x.sinceYear) IS UNIQUE }""".stripMargin,
      graphType(
        Seq(
          edgeType(
            nodeTypeRefByLabel("Person"),
            "LIVES_IN",
            nodeTypeRefByLabel("City"),
            propertyType("street", StringType(isNullable = true)),
            propertyType("streetNumber", StringType(isNullable = true))
          ),
          edgeType(
            nodeTypeRefByLabel("Person"),
            "KNOWS",
            nodeTypeRefByLabel("Person"),
            propertyType("sinceMonth", IntegerType(isNullable = true)),
            propertyType("sinceYear", IntegerType(isNullable = true))
          )
        ),
        Seq(
          uniquenessConstraint(
            identifyingEdgeTypeRef("LIVES_IN", "x"),
            ArraySeq(prop("x", "street"), prop("x", "streetNumber"))
          ),
          uniquenessConstraint(
            identifyingEdgeTypeRef("KNOWS", "x"),
            ArraySeq(prop("x", "sinceMonth"), prop("x", "sinceYear"))
          )
        )
      ),
      """{
        |  (:`Person`)-[:`KNOWS` => {`sinceMonth` :: INTEGER, `sinceYear` :: INTEGER}]->(:`Person`),
        |  (:`Person`)-[:`LIVES_IN` => {`street` :: STRING, `streetNumber` :: STRING}]->(:`City`),
        |  CONSTRAINT FOR ()-[`x`:`KNOWS` =>]->() REQUIRE (`x`.`sinceMonth`, `x`.`sinceYear`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`x`:`LIVES_IN` =>]->() REQUIRE (`x`.`street`, `x`.`streetNumber`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person`)-[:`KNOWS` => {`sinceMonth` :: INTEGER, `sinceYear` :: INTEGER}]->(:`Person`),
        |  (:`Person`)-[:`LIVES_IN` => {`street` :: STRING, `streetNumber` :: STRING}]->(:`City`),
        |  CONSTRAINT FOR ()-[`r`:`KNOWS` =>]->() REQUIRE (`r`.`sinceMonth`, `r`.`sinceYear`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`street`, `r`.`streetNumber`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "MET-PE-KUC-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person)-[l:LIVES_IN => {street :: STRING, streetNumber :: STRING, since :: DATE}]->(:City),
        |  (:Person)-[:KNOWS => {sinceMonth :: INT, sinceYear :: INT, since :: DATE}]->(:Person),
        |  CONSTRAINT FOR ()-[l]->() REQUIRE (l.since) IS KEY,
        |  CONSTRAINT FOR ()-[x:LIVES_IN =>]->() REQUIRE (x.street, x.streetNumber) IS UNIQUE,
        |  CONSTRAINT FOR ()-[x:KNOWS =>]->() REQUIRE (x.sinceMonth, x.sinceYear) IS UNIQUE,
        |  CONSTRAINT FOR ()-[x:KNOWS =>]->() REQUIRE (x.since) IS KEY }""".stripMargin,
      graphType(
        Seq(
          edgeType(
            nodeTypeRefByLabel("Person"),
            "LIVES_IN",
            "l",
            nodeTypeRefByLabel("City"),
            propertyType("street", StringType(isNullable = true)),
            propertyType("streetNumber", StringType(isNullable = true)),
            propertyType("since", DateType(isNullable = true))
          ),
          edgeType(
            nodeTypeRefByLabel("Person"),
            "KNOWS",
            nodeTypeRefByLabel("Person"),
            propertyType("sinceMonth", IntegerType(isNullable = true)),
            propertyType("sinceYear", IntegerType(isNullable = true)),
            propertyType("since", DateType(isNullable = true))
          )
        ),
        Seq(
          keyConstraint(
            edgeTypeRefByVar("l"),
            ArraySeq(prop("l", "since"))
          ),
          uniquenessConstraint(
            identifyingEdgeTypeRef("LIVES_IN", "x"),
            ArraySeq(prop("x", "street"), prop("x", "streetNumber"))
          ),
          uniquenessConstraint(
            identifyingEdgeTypeRef("KNOWS", "x"),
            ArraySeq(prop("x", "sinceMonth"), prop("x", "sinceYear"))
          ),
          keyConstraint(
            identifyingEdgeTypeRef("KNOWS", "x"),
            ArraySeq(prop("x", "since"))
          )
        )
      ),
      """{
        |  (:`Person`)-[:`KNOWS` => {`since` :: DATE, `sinceMonth` :: INTEGER, `sinceYear` :: INTEGER}]->(:`Person`),
        |  (:`Person`)-[`l`:`LIVES_IN` => {`since` :: DATE, `street` :: STRING, `streetNumber` :: STRING}]->(:`City`),
        |  CONSTRAINT FOR ()-[`x`:`KNOWS` =>]->() REQUIRE (`x`.`since`) IS KEY,
        |  CONSTRAINT FOR ()-[`x`:`KNOWS` =>]->() REQUIRE (`x`.`sinceMonth`, `x`.`sinceYear`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`x`:`LIVES_IN` =>]->() REQUIRE (`x`.`street`, `x`.`streetNumber`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person`)-[:`KNOWS` => {`since` :: DATE, `sinceMonth` :: INTEGER, `sinceYear` :: INTEGER}]->(:`Person`),
        |  (:`Person`)-[:`LIVES_IN` => {`since` :: DATE, `street` :: STRING, `streetNumber` :: STRING}]->(:`City`),
        |  CONSTRAINT FOR ()-[`r`:`KNOWS` =>]->() REQUIRE (`r`.`since`) IS KEY,
        |  CONSTRAINT FOR ()-[`r`:`KNOWS` =>]->() REQUIRE (`r`.`sinceMonth`, `r`.`sinceYear`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`street`, `r`.`streetNumber`) IS UNIQUE
        |}""".stripMargin
    )
  )

  private def udc(): Seq[GraphTypeTestCase] = Seq(
    GraphTypeTestCase(
      "UDC-PE-SCANT-1-1",
      """ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR (s: Student) REQUIRE s.name IS KEY }""",
      graphType(
        Seq(),
        Seq(
          keyConstraint(
            nodeTypeRefByLabel("Student", "s"),
            ArraySeq(prop("s", "name"))
          )
        )
      ),
      """{
        |  CONSTRAINT FOR (`s`:`Student`) REQUIRE (`s`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  CONSTRAINT FOR (`n`:`Student`) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCANT-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  CONSTRAINT FOR (s: Student) REQUIRE s.name IS KEY }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          )
        ),
        Seq(
          keyConstraint(
            nodeTypeRefByLabel("Student", "s"),
            ArraySeq(prop("s", "name"))
          )
        )
      ),
      """{
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`s`:`Student`) REQUIRE (`s`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Student`) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCANT-3-1",
      """ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR (s: Student) REQUIRE s.name IS UNIQUE }""",
      graphType(
        Seq(),
        Seq(
          uniquenessConstraint(
            nodeTypeRefByLabel("Student", "s"),
            ArraySeq(prop("s", "name"))
          )
        )
      ),
      """{
        |  CONSTRAINT FOR (`s`:`Student`) REQUIRE (`s`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  CONSTRAINT FOR (`n`:`Student`) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCANT-4-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  CONSTRAINT FOR (s: Student) REQUIRE s.name IS UNIQUE }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          )
        ),
        Seq(
          uniquenessConstraint(
            nodeTypeRefByLabel("Student", "s"),
            ArraySeq(prop("s", "name"))
          )
        )
      ),
      """{
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`s`:`Student`) REQUIRE (`s`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Student`) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCAET-1-1",
      """ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR ()-[k:KNOWS]->() REQUIRE k.since IS KEY }""",
      graphType(
        Seq(),
        Seq(
          keyConstraint(
            edgeTypeRefByLabel("KNOWS", "k"),
            ArraySeq(prop("k", "since"))
          )
        )
      ),
      """{
        |  CONSTRAINT FOR ()-[`k`:`KNOWS`]->() REQUIRE (`k`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  CONSTRAINT FOR ()-[`r`:`KNOWS`]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCAET-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person)-[:LIVES_IN =>]->(:City),
        |  CONSTRAINT FOR ()-[k:KNOWS]->() REQUIRE k.since IS KEY }""".stripMargin,
      graphType(
        Seq(
          edgeType(
            nodeTypeRefByLabel("Person"),
            "LIVES_IN",
            nodeTypeRefByLabel("City")
          )
        ),
        Seq(
          keyConstraint(
            edgeTypeRefByLabel("KNOWS", "k"),
            ArraySeq(prop("k", "since"))
          )
        )
      ),
      """{
        |  (:`Person`)-[:`LIVES_IN` =>]->(:`City`),
        |  CONSTRAINT FOR ()-[`k`:`KNOWS`]->() REQUIRE (`k`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`Person`)-[:`LIVES_IN` =>]->(:`City`),
        |  CONSTRAINT FOR ()-[`r`:`KNOWS`]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCAET-3-1",
      """ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR ()-[k:KNOWS]->() REQUIRE k.since IS UNIQUE }""",
      graphType(
        Seq(),
        Seq(
          uniquenessConstraint(
            edgeTypeRefByLabel("KNOWS", "k"),
            ArraySeq(prop("k", "since"))
          )
        )
      ),
      """{
        |  CONSTRAINT FOR ()-[`k`:`KNOWS`]->() REQUIRE (`k`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  CONSTRAINT FOR ()-[`r`:`KNOWS`]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCAET-4-1",
      """ALTER CURRENT GRAPH TYPE SET { (:Person)-[:LIVES_IN =>]->(:City),
        |  CONSTRAINT FOR ()-[k:KNOWS]->() REQUIRE k.since IS UNIQUE }""".stripMargin,
      graphType(
        Seq(
          edgeType(
            nodeTypeRefByLabel("Person"),
            "LIVES_IN",
            nodeTypeRefByLabel("City")
          )
        ),
        Seq(
          uniquenessConstraint(
            edgeTypeRefByLabel("KNOWS", "k"),
            ArraySeq(prop("k", "since"))
          )
        )
      ),
      """{
        |  (:`Person`)-[:`LIVES_IN` =>]->(:`City`),
        |  CONSTRAINT FOR ()-[`k`:`KNOWS`]->() REQUIRE (`k`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`Person`)-[:`LIVES_IN` =>]->(:`City`),
        |  CONSTRAINT FOR ()-[`r`:`KNOWS`]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING IS KEY, age :: INT}),
        |  (c :City => {name :: STRING}), (p)-[:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos)),
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
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING IS KEY}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-1-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}) REQUIRE p.name IS KEY,
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
      graphType(
        nodeTypeWithConstraints(
          "Person",
          "p",
          Set(KeyConstraint(ArraySeq(prop("p", "name")))(defaultPos)),
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
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}) REQUIRE (`p`.`name`) IS KEY,
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-1-3",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR (p) REQUIRE p.name IS KEY }""".stripMargin,
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
        Seq(keyConstraint(nodeTypeRefByVar("p"), ArraySeq(prop("p", "name"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-1-4",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR (p :Person => ) REQUIRE p.name IS KEY }""".stripMargin,
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
        Seq(keyConstraint(identifyingNodeTypeRef("Person", "p"), ArraySeq(prop("p", "name"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`:`Person` =>) REQUIRE (`p`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-1-5",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR (x :Person => ) REQUIRE x.name IS KEY }""".stripMargin,
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
        Seq(keyConstraint(identifyingNodeTypeRef("Person", "x"), ArraySeq(prop("x", "name"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`x`:`Person` =>) REQUIRE (`x`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-1-6",
      """ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR (p) REQUIRE p.name IS KEY, (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}) }""".stripMargin,
      graphType(
        Seq(
          edgeType(
            nodeTypeRefByVar("p"),
            "LIVES_IN",
            "l",
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
          ),
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
          )
        ),
        Seq(keyConstraint(nodeTypeRefByVar("p"), ArraySeq(prop("p", "name"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING NOT NULL IS KEY, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = false), PropertyInlineKeyConstraint()(defaultPos)),
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
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL IS KEY}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-2-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING NOT NULL, age :: INT}) REQUIRE p.name IS KEY,
        |  (c :City => {name :: STRING}), (p)-[l:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
      graphType(
        nodeTypeWithConstraints(
          "Person",
          "p",
          Set(KeyConstraint(ArraySeq(prop("p", "name")))(defaultPos)),
          propertyType("name", StringType(isNullable = false)),
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
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}) REQUIRE (`p`.`name`) IS KEY,
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-2-3",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING NOT NULL, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c), CONSTRAINT FOR (p) REQUIRE p.name IS KEY }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = false)),
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
        Seq(keyConstraint(nodeTypeRefByVar("p"), ArraySeq(prop("p", "name"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-2-4",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING NOT NULL, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c), CONSTRAINT FOR (p :Person => ) REQUIRE p.name IS KEY }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = false)),
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
        Seq(keyConstraint(identifyingNodeTypeRef("Person", "p"), ArraySeq(prop("p", "name"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`:`Person` =>) REQUIRE (`p`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-2-5",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING NOT NULL, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c), CONSTRAINT FOR (x :Person => ) REQUIRE x.name IS KEY }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = false)),
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
        Seq(keyConstraint(identifyingNodeTypeRef("Person", "x"), ArraySeq(prop("x", "name"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`x`:`Person` =>) REQUIRE (`x`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-2-6",
      """ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR (p) REQUIRE p.name IS KEY, (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  (p :Person => {name :: STRING NOT NULL, age :: INT}), (c :City => {name :: STRING}) }""".stripMargin,
      graphType(
        Seq(
          edgeType(
            nodeTypeRefByVar("p"),
            "LIVES_IN",
            "l",
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
          ),
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = false)),
            propertyType("age", IntegerType(isNullable = true))
          ),
          nodeType(
            "City",
            "c",
            propertyType("name", StringType(isNullable = true))
          )
        ),
        Seq(keyConstraint(nodeTypeRefByVar("p"), ArraySeq(prop("p", "name"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING IS UNIQUE, age :: INT}), (c :City => {name :: STRING}), (p)-[:LIVES_IN => {since :: DATE}]->(c) }""",
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true), PropertyInlineUniquenessConstraint()(defaultPos)),
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
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING IS UNIQUE}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-3-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}) REQUIRE p.name IS UNIQUE, (c :City => {name :: STRING}), (p)-[l:LIVES_IN => {since :: DATE}]->(c) }""",
      graphType(
        nodeTypeWithConstraints(
          "Person",
          "p",
          Set(UniquenessConstraint(
            ArraySeq(prop("p", "name", defaultPos))
          )(defaultPos)),
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
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}) REQUIRE (`p`.`name`) IS UNIQUE,
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-3-3",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}), (p)-[l:LIVES_IN => {since :: DATE}]->(c), CONSTRAINT FOR (p) REQUIRE p.name IS UNIQUE }""",
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
        Seq(uniquenessConstraint(nodeTypeRefByVar("p"), ArraySeq(prop("p", "name"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-3-4",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}), (p)-[l:LIVES_IN => {since :: DATE}]->(c), CONSTRAINT FOR (p :Person => ) REQUIRE p.name IS UNIQUE }""",
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
        Seq(uniquenessConstraint(identifyingNodeTypeRef("Person", "p"), ArraySeq(prop("p", "name"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`:`Person` =>) REQUIRE (`p`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-3-5",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR (x :Person => ) REQUIRE x.name IS UNIQUE }""".stripMargin,
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
        Seq(uniquenessConstraint(identifyingNodeTypeRef("Person", "x"), ArraySeq(prop("x", "name"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`x`:`Person` =>) REQUIRE (`x`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-3-6",
      """ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR (p) REQUIRE p.name IS UNIQUE,
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}) }""".stripMargin,
      graphType(
        Seq(
          edgeType(
            nodeTypeRefByVar("p"),
            "LIVES_IN",
            "l",
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
          ),
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
          )
        ),
        Seq(uniquenessConstraint(nodeTypeRefByVar("p"), ArraySeq(prop("p", "name"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-4-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING NOT NULL IS UNIQUE, age :: INT}), (c :City => {name :: STRING}), (p)-[:LIVES_IN => {since :: DATE}]->(c) }""",
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = false), PropertyInlineUniquenessConstraint()(defaultPos)),
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
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL IS UNIQUE}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-4-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING NOT NULL, age :: INT}) REQUIRE p.name IS UNIQUE, (c :City => {name :: STRING}), (p)-[l:LIVES_IN => {since :: DATE}]->(c) }""",
      graphType(
        nodeTypeWithConstraints(
          "Person",
          "p",
          Set(UniquenessConstraint(
            ArraySeq(prop("p", "name", defaultPos))
          )(defaultPos)),
          propertyType("name", StringType(isNullable = false)),
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
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}) REQUIRE (`p`.`name`) IS UNIQUE,
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-4-3",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING NOT NULL, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c), CONSTRAINT FOR (p) REQUIRE p.name IS UNIQUE }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = false)),
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
        Seq(uniquenessConstraint(nodeTypeRefByVar("p"), ArraySeq(prop("p", "name"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-4-4",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING NOT NULL, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c), CONSTRAINT FOR (p :Person => ) REQUIRE p.name IS UNIQUE }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = false)),
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
        Seq(uniquenessConstraint(identifyingNodeTypeRef("Person", "p"), ArraySeq(prop("p", "name"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`:`Person` =>) REQUIRE (`p`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-4-5",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING NOT NULL, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c), CONSTRAINT FOR (x :Person => ) REQUIRE x.name IS UNIQUE }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = false)),
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
        Seq(uniquenessConstraint(identifyingNodeTypeRef("Person", "x"), ArraySeq(prop("x", "name"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`x`:`Person` =>) REQUIRE (`x`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-4-6",
      """ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR (p) REQUIRE p.name IS UNIQUE, (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  (p :Person => {name :: STRING NOT NULL, age :: INT}), (c :City => {name :: STRING}) }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = false)),
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
        Seq(uniquenessConstraint(nodeTypeRefByVar("p"), ArraySeq(prop("p", "name"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING NOT NULL}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-5-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}) REQUIRE p.address IS KEY,
        |  (c :City => {name :: STRING}), (p)-[l:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
      graphType(
        nodeTypeWithConstraints(
          "Person",
          "p",
          Set(KeyConstraint(
            ArraySeq(prop("p", "address", defaultPos))
          )(defaultPos)),
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
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}) REQUIRE (`p`.`address`) IS KEY,
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`address`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-5-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c), CONSTRAINT FOR (p) REQUIRE p.address IS KEY }""".stripMargin,
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
        Seq(keyConstraint(nodeTypeRefByVar("p"), ArraySeq(prop("p", "address"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`address`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`address`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-6-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}) REQUIRE p.address IS UNIQUE,
        |  (c :City => {name :: STRING}), (p)-[l:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
      graphType(
        nodeTypeWithConstraints(
          "Person",
          "p",
          Set(UniquenessConstraint(
            ArraySeq(prop("p", "address", defaultPos))
          )(defaultPos)),
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
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}) REQUIRE (`p`.`address`) IS UNIQUE,
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`address`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTSP-6-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c), CONSTRAINT FOR (p) REQUIRE p.address IS UNIQUE }""".stripMargin,
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
        Seq(uniquenessConstraint(nodeTypeRefByVar("p"), ArraySeq(prop("p", "address"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`address`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`address`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[:LIVES_IN => {since :: DATE IS KEY}]->(c) }""".stripMargin,
      graphType(
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
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE IS KEY}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-1-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}), (p)-[l:LIVES_IN => {since :: DATE}]->(c) REQUIRE l.since IS KEY }""",
      graphType(
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
        edgeTypeWithConstraints(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          "l",
          nodeTypeRefByVar("c"),
          Set(KeyConstraint(
            ArraySeq(prop("l", "since", defaultPos))
          )(defaultPos)),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`) REQUIRE (`l`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-1-3",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR ()-[l]->() REQUIRE l.since IS KEY }""".stripMargin,
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
        Seq(keyConstraint(edgeTypeRefByVar("l"), ArraySeq(prop("l", "since"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-1-4",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c), CONSTRAINT FOR ()-[l:LIVES_IN =>]->() REQUIRE l.since IS KEY }""".stripMargin,
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
        Seq(keyConstraint(identifyingEdgeTypeRef("LIVES_IN", "l"), ArraySeq(prop("l", "since"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`:`LIVES_IN` =>]->() REQUIRE (`l`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-1-5",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR ()-[x:LIVES_IN =>]->() REQUIRE x.since IS KEY }""".stripMargin,
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
        Seq(keyConstraint(identifyingEdgeTypeRef("LIVES_IN", "x"), ArraySeq(prop("x", "since"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`x`:`LIVES_IN` =>]->() REQUIRE (`x`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-1-6",
      """ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR ()-[l]->() REQUIRE l.since IS KEY,
        |  (p :Person => {name :: STRING, age :: INT}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  (c :City => {name :: STRING}) }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          ),
          edgeType(
            nodeTypeRefByVar("p"),
            "LIVES_IN",
            "l",
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
          ),
          nodeType(
            "City",
            "c",
            propertyType("name", StringType(isNullable = true))
          )
        ),
        Seq(keyConstraint(edgeTypeRefByVar("l"), ArraySeq(prop("l", "since"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}), (p)-[:LIVES_IN => {since :: DATE NOT NULL IS KEY}]->(c) }""",
      graphType(
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
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = false), PropertyInlineKeyConstraint()(defaultPos))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE NOT NULL IS KEY}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-2-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE NOT NULL}]->(c)  REQUIRE l.since IS KEY }""".stripMargin,
      graphType(
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
        edgeTypeWithConstraints(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          "l",
          nodeTypeRefByVar("c"),
          Set(KeyConstraint(
            ArraySeq(prop("l", "since", defaultPos))
          )(defaultPos)),
          propertyType("since", DateType(isNullable = false))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(`c`) REQUIRE (`l`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-2-3",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE NOT NULL}]->(c), CONSTRAINT FOR ()-[l]->() REQUIRE l.since IS KEY }""".stripMargin,
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
            propertyType("since", DateType(isNullable = false))
          )
        ),
        Seq(keyConstraint(edgeTypeRefByVar("l"), ArraySeq(prop("l", "since"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-2-4",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE NOT NULL}]->(c), CONSTRAINT FOR ()-[l:LIVES_IN =>]->() REQUIRE l.since IS KEY }""".stripMargin,
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
            propertyType("since", DateType(isNullable = false))
          )
        ),
        Seq(keyConstraint(identifyingEdgeTypeRef("LIVES_IN", "l"), ArraySeq(prop("l", "since"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`:`LIVES_IN` =>]->() REQUIRE (`l`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-2-5",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE NOT NULL}]->(c), CONSTRAINT FOR ()-[x:LIVES_IN =>]->() REQUIRE x.since IS KEY }""".stripMargin,
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
            propertyType("since", DateType(isNullable = false))
          )
        ),
        Seq(keyConstraint(identifyingEdgeTypeRef("LIVES_IN", "x"), ArraySeq(prop("x", "since"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(`c`),
        |  CONSTRAINT FOR ()-[`x`:`LIVES_IN` =>]->() REQUIRE (`x`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-2-6",
      """ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR ()-[l]->() REQUIRE l.since IS KEY,
        |  (p :Person => {name :: STRING, age :: INT}),
        |  (p)-[l:LIVES_IN => {since :: DATE NOT NULL}]->(c),
        |  (c :City => {name :: STRING}) }""".stripMargin,
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
            propertyType("since", DateType(isNullable = false))
          )
        ),
        Seq(keyConstraint(edgeTypeRefByVar("l"), ArraySeq(prop("l", "since"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[:LIVES_IN => {since :: DATE IS UNIQUE}]->(c) }""".stripMargin,
      graphType(
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
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true), PropertyInlineUniquenessConstraint()(defaultPos))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE IS UNIQUE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-3-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}), (p)-[l:LIVES_IN => {since :: DATE}]->(c) REQUIRE l.since IS UNIQUE }""",
      graphType(
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
        edgeTypeWithConstraints(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          "l",
          nodeTypeRefByVar("c"),
          Set(UniquenessConstraint(
            ArraySeq(prop("l", "since", defaultPos))
          )(defaultPos)),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`) REQUIRE (`l`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-3-3",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR ()-[l]->() REQUIRE l.since IS UNIQUE }""".stripMargin,
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
        Seq(uniquenessConstraint(edgeTypeRefByVar("l"), ArraySeq(prop("l", "since"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-3-4",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR ()-[l:LIVES_IN =>]->() REQUIRE l.since IS UNIQUE }""".stripMargin,
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
        Seq(uniquenessConstraint(identifyingEdgeTypeRef("LIVES_IN", "l"), ArraySeq(prop("l", "since"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`:`LIVES_IN` =>]->() REQUIRE (`l`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-3-5",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR ()-[x:LIVES_IN =>]->() REQUIRE x.since IS UNIQUE }""".stripMargin,
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
        Seq(uniquenessConstraint(identifyingEdgeTypeRef("LIVES_IN", "x"), ArraySeq(prop("x", "since"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`x`:`LIVES_IN` =>]->() REQUIRE (`x`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-3-6",
      """ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR ()-[l]->() REQUIRE l.since IS UNIQUE,
        |  (p :Person => {name :: STRING, age :: INT}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  (c :City => {name :: STRING}) }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          ),
          edgeType(
            nodeTypeRefByVar("p"),
            "LIVES_IN",
            "l",
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
          ),
          nodeType(
            "City",
            "c",
            propertyType("name", StringType(isNullable = true))
          )
        ),
        Seq(uniquenessConstraint(edgeTypeRefByVar("l"), ArraySeq(prop("l", "since"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-4-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[:LIVES_IN => {since :: DATE NOT NULL IS UNIQUE}]->(c) }""".stripMargin,
      graphType(
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
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = false), PropertyInlineUniquenessConstraint()(defaultPos))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE NOT NULL IS UNIQUE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-4-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |   (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE NOT NULL}]->(c)
        |    REQUIRE l.since IS UNIQUE }""".stripMargin,
      graphType(
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
        edgeTypeWithConstraints(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          "l",
          nodeTypeRefByVar("c"),
          Set(UniquenessConstraint(
            ArraySeq(prop("l", "since", defaultPos))
          )(defaultPos)),
          propertyType("since", DateType(isNullable = false))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(`c`) REQUIRE (`l`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-4-3",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE NOT NULL}]->(c),
        |  CONSTRAINT FOR ()-[l]->() REQUIRE l.since IS UNIQUE }""".stripMargin,
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
            propertyType("since", DateType(isNullable = false))
          )
        ),
        Seq(uniquenessConstraint(edgeTypeRefByVar("l"), ArraySeq(prop("l", "since"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-4-4",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE NOT NULL}]->(c),
        |  CONSTRAINT FOR ()-[l:LIVES_IN =>]->() REQUIRE l.since IS UNIQUE }""".stripMargin,
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
            propertyType("since", DateType(isNullable = false))
          )
        ),
        Seq(uniquenessConstraint(identifyingEdgeTypeRef("LIVES_IN", "l"), ArraySeq(prop("l", "since"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`:`LIVES_IN` =>]->() REQUIRE (`l`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-4-5",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE NOT NULL}]->(c),
        |  CONSTRAINT FOR ()-[x:LIVES_IN =>]->() REQUIRE x.since IS UNIQUE }""".stripMargin,
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
            propertyType("since", DateType(isNullable = false))
          )
        ),
        Seq(uniquenessConstraint(identifyingEdgeTypeRef("LIVES_IN", "x"), ArraySeq(prop("x", "since"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(`c`),
        |  CONSTRAINT FOR ()-[`x`:`LIVES_IN` =>]->() REQUIRE (`x`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-4-6",
      """ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR ()-[l]->() REQUIRE l.since IS UNIQUE,
        |  (p :Person => {name :: STRING, age :: INT}),
        |  (p)-[l:LIVES_IN => {since :: DATE NOT NULL}]->(c),
        |  (c :City => {name :: STRING}) }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          ),
          edgeType(
            nodeTypeRefByVar("p"),
            "LIVES_IN",
            "l",
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = false))
          ),
          nodeType(
            "City",
            "c",
            propertyType("name", StringType(isNullable = true))
          )
        ),
        Seq(uniquenessConstraint(edgeTypeRefByVar("l"), ArraySeq(prop("l", "since"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-5-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c)  REQUIRE l.reason IS KEY }""".stripMargin,
      graphType(
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
        edgeTypeWithConstraints(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          "l",
          nodeTypeRefByVar("c"),
          Set(KeyConstraint(
            ArraySeq(prop("l", "reason", defaultPos))
          )(defaultPos)),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`) REQUIRE (`l`.`reason`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-5-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR ()-[l]->() REQUIRE l.reason IS KEY }""".stripMargin,
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
        Seq(keyConstraint(edgeTypeRefByVar("l"), ArraySeq(prop("l", "reason"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`reason`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`reason`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-6-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c)
        |    REQUIRE l.reason IS UNIQUE }""".stripMargin,
      graphType(
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
        edgeTypeWithConstraints(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          "l",
          nodeTypeRefByVar("c"),
          Set(UniquenessConstraint(
            ArraySeq(prop("l", "reason", defaultPos))
          )(defaultPos)),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`) REQUIRE (`l`.`reason`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETSP-6-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR ()-[l]->() REQUIRE l.reason IS UNIQUE }""".stripMargin,
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
        Seq(uniquenessConstraint(edgeTypeRefByVar("l"), ArraySeq(prop("l", "reason"))))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`reason`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`reason`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTMP-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}) REQUIRE (p.name, p.age) IS KEY, (c :City => {name :: STRING}), (p)-[:LIVES_IN => {since :: DATE}]->(c) }""",
      graphType(
        nodeTypeWithConstraints(
          "Person",
          "p",
          Set(KeyConstraint(
            ArraySeq(prop("p", "name", defaultPos), prop("p", "age", defaultPos))
          )(defaultPos)),
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
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}) REQUIRE (`p`.`name`, `p`.`age`) IS KEY,
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`age`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTMP-1-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), CONSTRAINT FOR (p) REQUIRE (p.name, p.age) IS KEY, (c :City => {name :: STRING}), (p)-[:LIVES_IN => {since :: DATE}]->(c) }""",
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
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
          )
        ),
        Seq(keyConstraint(
          nodeTypeRefByVar("p"),
          ArraySeq(prop("p", "name", defaultPos), prop("p", "age", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`, `p`.`age`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`age`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTMP-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}) REQUIRE (p.name, p.age) IS UNIQUE,
        |  (c :City => {name :: STRING}),
        |  (p)-[:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
      graphType(
        nodeTypeWithConstraints(
          "Person",
          "p",
          Set(UniquenessConstraint(
            ArraySeq(prop("p", "name", defaultPos), prop("p", "age", defaultPos))
          )(defaultPos)),
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
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}) REQUIRE (`p`.`name`, `p`.`age`) IS UNIQUE,
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`age`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTMP-2-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  CONSTRAINT FOR (p) REQUIRE (p.name, p.age) IS UNIQUE,
        |  (c :City => {name :: STRING}),
        |  (p)-[:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
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
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
          )
        ),
        Seq(uniquenessConstraint(
          nodeTypeRefByVar("p"),
          ArraySeq(prop("p", "name", defaultPos), prop("p", "age", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`, `p`.`age`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`age`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTMP-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING}) REQUIRE (p.name, p.age) IS KEY, (c :City => {name :: STRING}), (p)-[:LIVES_IN => {since :: DATE}]->(c) }""",
      graphType(
        nodeTypeWithConstraints(
          "Person",
          "p",
          Set(KeyConstraint(ArraySeq(prop("p", "name"), prop("p", "age")))(defaultPos)),
          propertyType("name", StringType(isNullable = true))
        ),
        nodeType(
          "City",
          "c",
          propertyType("name", StringType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`name` :: STRING}) REQUIRE (`p`.`name`, `p`.`age`) IS KEY,
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`age`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTMP-3-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING}), CONSTRAINT FOR (p) REQUIRE (p.name, p.age) IS KEY,
        |  (c :City => {name :: STRING}),
        |  (p)-[:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = true))
          ),
          nodeType(
            "City",
            "c",
            propertyType("name", StringType(isNullable = true))
          ),
          edgeType(
            nodeTypeRefByVar("p"),
            "LIVES_IN",
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
          )
        ),
        Seq(keyConstraint(
          nodeTypeRefByVar("p"),
          ArraySeq(prop("p", "name", defaultPos), prop("p", "age", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`name` :: STRING}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`, `p`.`age`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`age`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTMP-4-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING}) REQUIRE (p.name, p.age) IS UNIQUE,
        |  (c :City => {name :: STRING}),
        |  (p)-[:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
      graphType(
        nodeTypeWithConstraints(
          "Person",
          "p",
          Set(UniquenessConstraint(ArraySeq(prop("p", "name"), prop("p", "age")))(defaultPos)),
          propertyType("name", StringType(isNullable = true))
        ),
        nodeType(
          "City",
          "c",
          propertyType("name", StringType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`name` :: STRING}) REQUIRE (`p`.`name`, `p`.`age`) IS UNIQUE,
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`age`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTMP-4-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING}),
        |  CONSTRAINT FOR (p) REQUIRE (p.name, p.age) IS UNIQUE,
        |  (c :City => {name :: STRING}),
        |  (p)-[:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = true))
          ),
          nodeType(
            "City",
            "c",
            propertyType("name", StringType(isNullable = true))
          ),
          edgeType(
            nodeTypeRefByVar("p"),
            "LIVES_IN",
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
          )
        ),
        Seq(uniquenessConstraint(
          nodeTypeRefByVar("p"),
          ArraySeq(prop("p", "name", defaultPos), prop("p", "age", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`name` :: STRING}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`, `p`.`age`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`age`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTMP-5-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING}) REQUIRE (p.address, p.age) IS KEY, (c :City => {name :: STRING}), (p)-[:LIVES_IN => {since :: DATE}]->(c) }""",
      graphType(
        nodeTypeWithConstraints(
          "Person",
          "p",
          Set(KeyConstraint(ArraySeq(prop("p", "address"), prop("p", "age")))(defaultPos)),
          propertyType("name", StringType(isNullable = true))
        ),
        nodeType(
          "City",
          "c",
          propertyType("name", StringType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`name` :: STRING}) REQUIRE (`p`.`address`, `p`.`age`) IS KEY,
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`address`, `n`.`age`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTMP-5-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING}),
        |  CONSTRAINT FOR (p) REQUIRE (p.address, p.age) IS KEY,
        |  (c :City => {name :: STRING}),
        |  (p)-[:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = true))
          ),
          nodeType(
            "City",
            "c",
            propertyType("name", StringType(isNullable = true))
          ),
          edgeType(
            nodeTypeRefByVar("p"),
            "LIVES_IN",
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
          )
        ),
        Seq(keyConstraint(
          nodeTypeRefByVar("p"),
          ArraySeq(prop("p", "address", defaultPos), prop("p", "age", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`name` :: STRING}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`address`, `p`.`age`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`address`, `n`.`age`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTMP-6-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING}) REQUIRE (p.address, p.age) IS UNIQUE, (c :City => {name :: STRING}), (p)-[:LIVES_IN => {since :: DATE}]->(c) }""",
      graphType(
        nodeTypeWithConstraints(
          "Person",
          "p",
          Set(UniquenessConstraint(ArraySeq(prop("p", "address"), prop("p", "age")))(defaultPos)),
          propertyType("name", StringType(isNullable = true))
        ),
        nodeType(
          "City",
          "c",
          propertyType("name", StringType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`name` :: STRING}) REQUIRE (`p`.`address`, `p`.`age`) IS UNIQUE,
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`address`, `n`.`age`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCNTMP-6-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING}),
        |  CONSTRAINT FOR (p) REQUIRE (p.address, p.age) IS UNIQUE,
        |  (c :City => {name :: STRING}),
        |  (p)-[:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = true))
          ),
          nodeType(
            "City",
            "c",
            propertyType("name", StringType(isNullable = true))
          ),
          edgeType(
            nodeTypeRefByVar("p"),
            "LIVES_IN",
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
          )
        ),
        Seq(uniquenessConstraint(
          nodeTypeRefByVar("p"),
          ArraySeq(prop("p", "address", defaultPos), prop("p", "age", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`name` :: STRING}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`address`, `p`.`age`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`address`, `n`.`age`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETMP-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}), (p)-[l:LIVES_IN => {since :: DATE, address :: STRING, rating :: INT}]->(c) REQUIRE (l.address, l.since) IS KEY }""".stripMargin,
      graphType(
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
        edgeTypeWithConstraints(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          "l",
          nodeTypeRefByVar("c"),
          Set(KeyConstraint(ArraySeq(prop("l", "address"), prop("l", "since")))(defaultPos)),
          propertyType("since", DateType(isNullable = true)),
          propertyType("address", StringType(isNullable = true)),
          propertyType("rating", IntegerType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`address` :: STRING, `rating` :: INTEGER, `since` :: DATE}]->(`c`) REQUIRE (`l`.`address`, `l`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`address` :: STRING, `rating` :: INTEGER, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`, `r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETMP-1-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  CONSTRAINT FOR ()-[l]->() REQUIRE (l.address, l.since) IS KEY,
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE, address :: STRING, rating :: INT}]->(c) }""".stripMargin,
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
            propertyType("since", DateType(isNullable = true)),
            propertyType("address", StringType(isNullable = true)),
            propertyType("rating", IntegerType(isNullable = true))
          )
        ),
        Seq(keyConstraint(
          edgeTypeRefByVar("l"),
          ArraySeq(prop("l", "address", defaultPos), prop("l", "since", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`address` :: STRING, `rating` :: INTEGER, `since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`address`, `l`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`address` :: STRING, `rating` :: INTEGER, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`, `r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETMP-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}), (p)-[l:LIVES_IN => {since :: DATE, address :: STRING, rating :: INT}]->(c) REQUIRE (l.address, l.since) IS UNIQUE }""",
      graphType(
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
        edgeTypeWithConstraints(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          "l",
          nodeTypeRefByVar("c"),
          Set(UniquenessConstraint(ArraySeq(prop("l", "address"), prop("l", "since")))(defaultPos)),
          propertyType("since", DateType(isNullable = true)),
          propertyType("address", StringType(isNullable = true)),
          propertyType("rating", IntegerType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`address` :: STRING, `rating` :: INTEGER, `since` :: DATE}]->(`c`) REQUIRE (`l`.`address`, `l`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`address` :: STRING, `rating` :: INTEGER, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`, `r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETMP-2-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  CONSTRAINT FOR ()-[l]->() REQUIRE (l.address, l.since) IS UNIQUE,
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE, address :: STRING, rating :: INT}]->(c) }""".stripMargin,
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
            propertyType("since", DateType(isNullable = true)),
            propertyType("address", StringType(isNullable = true)),
            propertyType("rating", IntegerType(isNullable = true))
          )
        ),
        Seq(uniquenessConstraint(
          edgeTypeRefByVar("l"),
          ArraySeq(prop("l", "address", defaultPos), prop("l", "since", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`address` :: STRING, `rating` :: INTEGER, `since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`address`, `l`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`address` :: STRING, `rating` :: INTEGER, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`, `r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETMP-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE, rating :: INT}]->(c) REQUIRE (l.address, l.since) IS KEY }""".stripMargin,
      graphType(
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
        edgeTypeWithConstraints(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          "l",
          nodeTypeRefByVar("c"),
          Set(KeyConstraint(ArraySeq(prop("l", "address"), prop("l", "since")))(defaultPos)),
          propertyType("since", DateType(isNullable = true)),
          propertyType("rating", IntegerType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`rating` :: INTEGER, `since` :: DATE}]->(`c`) REQUIRE (`l`.`address`, `l`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`rating` :: INTEGER, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`, `r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETMP-3-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  CONSTRAINT FOR ()-[l]->() REQUIRE (l.address, l.since) IS KEY,
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE, rating :: INT}]->(c) }""".stripMargin,
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
            propertyType("since", DateType(isNullable = true)),
            propertyType("rating", IntegerType(isNullable = true))
          )
        ),
        Seq(keyConstraint(
          edgeTypeRefByVar("l"),
          ArraySeq(prop("l", "address", defaultPos), prop("l", "since", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`rating` :: INTEGER, `since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`address`, `l`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`rating` :: INTEGER, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`, `r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETMP-4-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE, rating :: INT}]->(c) REQUIRE (l.address, l.since) IS UNIQUE }""".stripMargin,
      graphType(
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
        edgeTypeWithConstraints(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          "l",
          nodeTypeRefByVar("c"),
          Set(UniquenessConstraint(ArraySeq(prop("l", "address"), prop("l", "since")))(defaultPos)),
          propertyType("since", DateType(isNullable = true)),
          propertyType("rating", IntegerType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`rating` :: INTEGER, `since` :: DATE}]->(`c`) REQUIRE (`l`.`address`, `l`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`rating` :: INTEGER, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`, `r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETMP-4-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  CONSTRAINT FOR ()-[l]->() REQUIRE (l.address, l.since) IS UNIQUE,
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE, rating :: INT}]->(c) }""".stripMargin,
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
            propertyType("since", DateType(isNullable = true)),
            propertyType("rating", IntegerType(isNullable = true))
          )
        ),
        Seq(uniquenessConstraint(
          edgeTypeRefByVar("l"),
          ArraySeq(prop("l", "address", defaultPos), prop("l", "since", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`rating` :: INTEGER, `since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`address`, `l`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`rating` :: INTEGER, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`, `r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETMP-5-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE, rating :: INT}]->(c) REQUIRE (l.address, l.taxNo) IS KEY }""".stripMargin,
      graphType(
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
        edgeTypeWithConstraints(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          "l",
          nodeTypeRefByVar("c"),
          Set(KeyConstraint(ArraySeq(prop("l", "address"), prop("l", "taxNo")))(defaultPos)),
          propertyType("since", DateType(isNullable = true)),
          propertyType("rating", IntegerType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`rating` :: INTEGER, `since` :: DATE}]->(`c`) REQUIRE (`l`.`address`, `l`.`taxNo`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`rating` :: INTEGER, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`, `r`.`taxNo`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETMP-5-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  CONSTRAINT FOR ()-[l]->() REQUIRE (l.address, l.taxNo) IS KEY,
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE, rating :: INT}]->(c) }""".stripMargin,
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
            propertyType("since", DateType(isNullable = true)),
            propertyType("rating", IntegerType(isNullable = true))
          )
        ),
        Seq(keyConstraint(
          edgeTypeRefByVar("l"),
          ArraySeq(prop("l", "address", defaultPos), prop("l", "taxNo", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`rating` :: INTEGER, `since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`address`, `l`.`taxNo`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`rating` :: INTEGER, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`, `r`.`taxNo`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETMP-6-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE, rating :: INT}]->(c) REQUIRE (l.address, l.taxNo) IS UNIQUE }""".stripMargin,
      graphType(
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
        edgeTypeWithConstraints(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          "l",
          nodeTypeRefByVar("c"),
          Set(UniquenessConstraint(ArraySeq(prop("l", "address"), prop("l", "taxNo")))(defaultPos)),
          propertyType("since", DateType(isNullable = true)),
          propertyType("rating", IntegerType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`rating` :: INTEGER, `since` :: DATE}]->(`c`) REQUIRE (`l`.`address`, `l`.`taxNo`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`rating` :: INTEGER, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`, `r`.`taxNo`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCCETMP-6-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  CONSTRAINT FOR ()-[l]->() REQUIRE (l.address, l.taxNo) IS UNIQUE, (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE, rating :: INT}]->(c) }""".stripMargin,
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
            propertyType("since", DateType(isNullable = true)),
            propertyType("rating", IntegerType(isNullable = true))
          )
        ),
        Seq(uniquenessConstraint(
          edgeTypeRefByVar("l"),
          ArraySeq(prop("l", "address", defaultPos), prop("l", "taxNo", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`rating` :: INTEGER, `since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`address`, `l`.`taxNo`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`rating` :: INTEGER, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`, `r`.`taxNo`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCWN-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c), CONSTRAINT myConstraint FOR (p) REQUIRE p.name IS KEY }""".stripMargin,
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
        Seq(keyConstraint(
          "myConstraint",
          nodeTypeRefByVar("p"),
          ArraySeq(prop("p", "name", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT `myConstraint` FOR (`p`) REQUIRE (`p`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT `myConstraint` FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCWN-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT myConstraint FOR (p) REQUIRE p.name IS UNIQUE }""".stripMargin,
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
        Seq(uniquenessConstraint(
          "myConstraint",
          nodeTypeRefByVar("p"),
          ArraySeq(prop("p", "name", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT `myConstraint` FOR (`p`) REQUIRE (`p`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT `myConstraint` FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCWN-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT myConstraint FOR ()-[l]->() REQUIRE l.since IS KEY }""".stripMargin,
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
        Seq(keyConstraint(
          "myConstraint",
          edgeTypeRefByVar("l"),
          ArraySeq(prop("l", "since", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT `myConstraint` FOR ()-[`l`]->() REQUIRE (`l`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT `myConstraint` FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCWN-4-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT myConstraint FOR ()-[l]->() REQUIRE l.since IS UNIQUE }""".stripMargin,
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
        Seq(uniquenessConstraint(
          "myConstraint",
          edgeTypeRefByVar("l"),
          ArraySeq(prop("l", "since", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT `myConstraint` FOR ()-[`l`]->() REQUIRE (`l`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT `myConstraint` FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSNT-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING IS KEY, age :: INT IS KEY}), (c :City => {name :: STRING}), (p)-[:LIVES_IN => {since :: DATE}]->(c) }""",
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos)),
          propertyType("age", IntegerType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos))
        ),
        nodeType(
          "City",
          "c",
          propertyType("name", StringType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER IS KEY, `name` :: STRING IS KEY}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`age`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSNT-1-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT IS KEY}) REQUIRE p.name IS KEY, (c :City => {name :: STRING}), (p)-[:LIVES_IN => {since :: DATE}]->(c) }""",
      graphType(
        nodeTypeWithConstraints(
          "Person",
          "p",
          Set(KeyConstraint(ArraySeq(prop("p", "name")))(defaultPos)),
          propertyType("name", StringType(isNullable = true)),
          propertyType("age", IntegerType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos))
        ),
        nodeType(
          "City",
          "c",
          propertyType("name", StringType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER IS KEY, `name` :: STRING}) REQUIRE (`p`.`name`) IS KEY,
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`age`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSNT-1-3",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}) REQUIRE p.name IS KEY REQUIRE p.age IS KEY, (c :City => {name :: STRING}), (p)-[:LIVES_IN => {since :: DATE}]->(c) }""",
      graphType(
        nodeTypeWithConstraints(
          "Person",
          "p",
          Set(
            KeyConstraint(ArraySeq(prop("p", "name")))(defaultPos),
            KeyConstraint(ArraySeq(prop("p", "age")))(defaultPos)
          ),
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
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}) REQUIRE (`p`.`age`) IS KEY REQUIRE (`p`.`name`) IS KEY,
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`age`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSNT-1-4",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}) REQUIRE p.name IS KEY,
        |  (c :City => {name :: STRING}),
        |  CONSTRAINT FOR (p) REQUIRE p.age IS KEY,
        |  (p)-[:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
      graphType(
        Seq(
          nodeTypeWithConstraints(
            "Person",
            "p",
            Set(KeyConstraint(ArraySeq(prop("p", "name")))(defaultPos)),
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
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
          )
        ),
        Seq(keyConstraint(
          nodeTypeRefByVar("p"),
          ArraySeq(prop("p", "age", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}) REQUIRE (`p`.`name`) IS KEY,
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`age`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`age`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSNT-1-5",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  CONSTRAINT FOR (p) REQUIRE p.name IS KEY, CONSTRAINT FOR (p) REQUIRE p.age IS KEY,
        |  (c :City => {name :: STRING}),
        |  (p)-[:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
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
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
          )
        ),
        Seq(
          keyConstraint(
            nodeTypeRefByVar("p"),
            ArraySeq(prop("p", "name", defaultPos))
          ),
          keyConstraint(
            nodeTypeRefByVar("p"),
            ArraySeq(prop("p", "age", defaultPos))
          )
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`age`) IS KEY,
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`age`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSNT-1-6",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING IS KEY, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR (p) REQUIRE p.age IS KEY }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos)),
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
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
          )
        ),
        Seq(keyConstraint(
          nodeTypeRefByVar("p"),
          ArraySeq(prop("p", "age", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING IS KEY}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`age`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`age`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSNT-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING IS UNIQUE, age :: INT IS UNIQUE}),
        |  (c :City => {name :: STRING}),
        |  (p)-[:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true), PropertyInlineUniquenessConstraint()(defaultPos)),
          propertyType("age", IntegerType(isNullable = true), PropertyInlineUniquenessConstraint()(defaultPos))
        ),
        nodeType(
          "City",
          "c",
          propertyType("name", StringType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER IS UNIQUE, `name` :: STRING IS UNIQUE}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`age`) IS UNIQUE,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSNT-2-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT IS UNIQUE}) REQUIRE p.name IS UNIQUE,
        |  (c :City => {name :: STRING}),
        |  (p)-[:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
      graphType(
        nodeTypeWithConstraints(
          "Person",
          "p",
          Set(UniquenessConstraint(ArraySeq(prop("p", "name")))(defaultPos)),
          propertyType("name", StringType(isNullable = true)),
          propertyType("age", IntegerType(isNullable = true), PropertyInlineUniquenessConstraint()(defaultPos))
        ),
        nodeType(
          "City",
          "c",
          propertyType("name", StringType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER IS UNIQUE, `name` :: STRING}) REQUIRE (`p`.`name`) IS UNIQUE,
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`age`) IS UNIQUE,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSNT-2-3",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT})
        |   REQUIRE p.name IS UNIQUE
        |   REQUIRE p.age IS UNIQUE,
        |(c :City => {name :: STRING}),
        |  (p)-[:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
      graphType(
        nodeTypeWithConstraints(
          "Person",
          "p",
          Set(
            UniquenessConstraint(ArraySeq(prop("p", "name")))(defaultPos),
            UniquenessConstraint(ArraySeq(prop("p", "age")))(defaultPos)
          ),
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
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}) REQUIRE (`p`.`age`) IS UNIQUE REQUIRE (`p`.`name`) IS UNIQUE,
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`age`) IS UNIQUE,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSNT-2-4",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}) REQUIRE p.name IS UNIQUE,
        |  (c :City => {name :: STRING}),
        |  CONSTRAINT FOR (p) REQUIRE p.age IS UNIQUE,
        |  (p)-[:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
      graphType(
        Seq(
          nodeTypeWithConstraints(
            "Person",
            "p",
            Set(UniquenessConstraint(ArraySeq(prop("p", "name")))(defaultPos)),
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
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
          )
        ),
        Seq(uniquenessConstraint(
          nodeTypeRefByVar("p"),
          ArraySeq(prop("p", "age", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}) REQUIRE (`p`.`name`) IS UNIQUE,
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`age`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`age`) IS UNIQUE,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSNT-2-5",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  CONSTRAINT FOR (p) REQUIRE p.name IS UNIQUE,
        |  CONSTRAINT FOR (p) REQUIRE p.age IS UNIQUE,
        |  (c :City => {name :: STRING}),
        |  (p)-[:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
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
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
          )
        ),
        Seq(
          uniquenessConstraint(
            nodeTypeRefByVar("p"),
            ArraySeq(prop("p", "name", defaultPos))
          ),
          uniquenessConstraint(
            nodeTypeRefByVar("p"),
            ArraySeq(prop("p", "age", defaultPos))
          )
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`age`) IS UNIQUE,
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`age`) IS UNIQUE,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSNT-2-6",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING IS UNIQUE, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR (p) REQUIRE p.age IS UNIQUE }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = true), PropertyInlineUniquenessConstraint()(defaultPos)),
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
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
          )
        ),
        Seq(uniquenessConstraint(
          nodeTypeRefByVar("p"),
          ArraySeq(prop("p", "age", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING IS UNIQUE}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`age`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`age`) IS UNIQUE,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSET-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}), (p)-[l:LIVES_IN => {since :: DATE IS KEY, address :: STRING IS KEY}]->(c) }""",
      graphType(
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
          propertyType("since", DateType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos)),
          propertyType("address", StringType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`address` :: STRING IS KEY, `since` :: DATE IS KEY}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`address` :: STRING, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`) IS KEY,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSET-1-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}), (p)-[l:LIVES_IN => {since :: DATE, address :: STRING IS KEY}]->(c) REQUIRE l.since IS KEY }""",
      graphType(
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
        edgeTypeWithConstraints(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          "l",
          nodeTypeRefByVar("c"),
          Set(KeyConstraint(ArraySeq(prop("l", "since")))(defaultPos)),
          propertyType("since", DateType(isNullable = true)),
          propertyType("address", StringType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`address` :: STRING IS KEY, `since` :: DATE}]->(`c`) REQUIRE (`l`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`address` :: STRING, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`) IS KEY,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSET-1-3",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE, address :: STRING}]->(c) REQUIRE l.since IS KEY REQUIRE l.address IS KEY }""".stripMargin,
      graphType(
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
        edgeTypeWithConstraints(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          "l",
          nodeTypeRefByVar("c"),
          Set(
            KeyConstraint(ArraySeq(prop("l", "since")))(defaultPos),
            KeyConstraint(ArraySeq(prop("l", "address")))(defaultPos)
          ),
          propertyType("since", DateType(isNullable = true)),
          propertyType("address", StringType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`address` :: STRING, `since` :: DATE}]->(`c`) REQUIRE (`l`.`address`) IS KEY REQUIRE (`l`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`address` :: STRING, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`) IS KEY,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSET-1-4",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  CONSTRAINT FOR ()-[l]->() REQUIRE l.address IS KEY,
        |  (p)-[l:LIVES_IN => {since :: DATE, address :: STRING}]->(c) REQUIRE l.since IS KEY }""".stripMargin,
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
          edgeTypeWithConstraints(
            nodeTypeRefByVar("p"),
            "LIVES_IN",
            "l",
            nodeTypeRefByVar("c"),
            Set(
              KeyConstraint(ArraySeq(prop("l", "since")))(defaultPos)
            ),
            propertyType("since", DateType(isNullable = true)),
            propertyType("address", StringType(isNullable = true))
          )
        ),
        Seq(keyConstraint(
          edgeTypeRefByVar("l"),
          ArraySeq(prop("l", "address", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`address` :: STRING, `since` :: DATE}]->(`c`) REQUIRE (`l`.`since`) IS KEY,
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`address`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`address` :: STRING, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`) IS KEY,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSET-1-5",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE, address :: STRING}]->(c),
        |  CONSTRAINT FOR ()-[l]->() REQUIRE l.since IS KEY,
        |  CONSTRAINT FOR ()-[l]->() REQUIRE l.address IS KEY }""".stripMargin,
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
            propertyType("since", DateType(isNullable = true)),
            propertyType("address", StringType(isNullable = true))
          )
        ),
        Seq(
          keyConstraint(
            edgeTypeRefByVar("l"),
            ArraySeq(prop("l", "since", defaultPos))
          ),
          keyConstraint(
            edgeTypeRefByVar("l"),
            ArraySeq(prop("l", "address", defaultPos))
          )
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`address` :: STRING, `since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`address`) IS KEY,
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`address` :: STRING, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`) IS KEY,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSET-1-6",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  CONSTRAINT FOR ()-[l]->() REQUIRE l.address IS KEY,
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE IS KEY, address :: STRING}]->(c) }""".stripMargin,
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
            propertyType("since", DateType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos)),
            propertyType("address", StringType(isNullable = true))
          )
        ),
        Seq(keyConstraint(
          edgeTypeRefByVar("l"),
          ArraySeq(prop("l", "address", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`address` :: STRING, `since` :: DATE IS KEY}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`address`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`address` :: STRING, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`) IS KEY,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSET-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}), (p)-[l:LIVES_IN => {since :: DATE IS UNIQUE, address :: STRING IS UNIQUE}]->(c) }""",
      graphType(
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
          propertyType("since", DateType(isNullable = true), PropertyInlineUniquenessConstraint()(defaultPos)),
          propertyType("address", StringType(isNullable = true), PropertyInlineUniquenessConstraint()(defaultPos))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`address` :: STRING IS UNIQUE, `since` :: DATE IS UNIQUE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`address` :: STRING, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSET-2-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}), (p)-[l:LIVES_IN => {since :: DATE, address :: STRING IS UNIQUE}]->(c) REQUIRE l.since IS UNIQUE }""",
      graphType(
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
        edgeTypeWithConstraints(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          "l",
          nodeTypeRefByVar("c"),
          Set(UniquenessConstraint(ArraySeq(prop("l", "since")))(defaultPos)),
          propertyType("since", DateType(isNullable = true)),
          propertyType("address", StringType(isNullable = true), PropertyInlineUniquenessConstraint()(defaultPos))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`address` :: STRING IS UNIQUE, `since` :: DATE}]->(`c`) REQUIRE (`l`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`address` :: STRING, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSET-2-3",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE, address :: STRING}]->(c)
        |  REQUIRE l.since IS UNIQUE
        |  REQUIRE l.address IS UNIQUE }""".stripMargin,
      graphType(
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
        edgeTypeWithConstraints(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          "l",
          nodeTypeRefByVar("c"),
          Set(
            UniquenessConstraint(ArraySeq(prop("l", "since")))(defaultPos),
            UniquenessConstraint(ArraySeq(prop("l", "address")))(defaultPos)
          ),
          propertyType("since", DateType(isNullable = true)),
          propertyType("address", StringType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`address` :: STRING, `since` :: DATE}]->(`c`) REQUIRE (`l`.`address`) IS UNIQUE REQUIRE (`l`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`address` :: STRING, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSET-2-4",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  CONSTRAINT FOR ()-[l]->() REQUIRE l.address IS UNIQUE,
        |  (p)-[l:LIVES_IN => {since :: DATE, address :: STRING}]->(c) REQUIRE l.since IS UNIQUE }""".stripMargin,
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
          edgeTypeWithConstraints(
            nodeTypeRefByVar("p"),
            "LIVES_IN",
            "l",
            nodeTypeRefByVar("c"),
            Set(
              UniquenessConstraint(ArraySeq(prop("l", "since")))(defaultPos)
            ),
            propertyType("since", DateType(isNullable = true)),
            propertyType("address", StringType(isNullable = true))
          )
        ),
        Seq(uniquenessConstraint(
          edgeTypeRefByVar("l"),
          ArraySeq(prop("l", "address", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`address` :: STRING, `since` :: DATE}]->(`c`) REQUIRE (`l`.`since`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`address`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`address` :: STRING, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSET-2-5",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE, address :: STRING}]->(c),
        |  CONSTRAINT FOR ()-[l]->() REQUIRE l.since IS UNIQUE,
        |  CONSTRAINT FOR ()-[l]->() REQUIRE l.address IS UNIQUE }""".stripMargin,
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
            propertyType("since", DateType(isNullable = true)),
            propertyType("address", StringType(isNullable = true))
          )
        ),
        Seq(
          uniquenessConstraint(
            edgeTypeRefByVar("l"),
            ArraySeq(prop("l", "since", defaultPos))
          ),
          uniquenessConstraint(
            edgeTypeRefByVar("l"),
            ArraySeq(prop("l", "address", defaultPos))
          )
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`address` :: STRING, `since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`address`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`address` :: STRING, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFSET-2-6",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  CONSTRAINT FOR ()-[l]->() REQUIRE l.address IS UNIQUE,
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE IS UNIQUE, address :: STRING}]->(c) }""".stripMargin,
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
            propertyType("since", DateType(isNullable = true), PropertyInlineUniquenessConstraint()(defaultPos)),
            propertyType("address", StringType(isNullable = true))
          )
        ),
        Seq(uniquenessConstraint(
          edgeTypeRefByVar("l"),
          ArraySeq(prop("l", "address", defaultPos))
        ))
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`address` :: STRING, `since` :: DATE IS UNIQUE}]->(`c`),
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`address`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`address` :: STRING, `since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`address`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFD-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING IS KEY, age :: INT}), (c :City => {name :: STRING}), (p)-[:LIVES_IN => {since :: DATE IS KEY}]->(c) }""",
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos)),
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
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING IS KEY}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE IS KEY}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFD-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING IS UNIQUE, age :: INT}), (c :City => {name :: STRING}), (p)-[:LIVES_IN => {since :: DATE IS KEY}]->(c) }""",
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true), PropertyInlineUniquenessConstraint()(defaultPos)),
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
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING IS UNIQUE}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE IS KEY}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFD-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING IS KEY, age :: INT}), (c :City => {name :: STRING}), (p)-[:LIVES_IN => {since :: DATE IS UNIQUE}]->(c) }""",
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos)),
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
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true), PropertyInlineUniquenessConstraint()(defaultPos))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING IS KEY}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE IS UNIQUE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFD-4-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING IS UNIQUE, age :: INT}), (c :City => {name :: STRING}), (p)-[:LIVES_IN => {since :: DATE IS UNIQUE}]->(c) }""",
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true), PropertyInlineUniquenessConstraint()(defaultPos)),
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
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true), PropertyInlineUniquenessConstraint()(defaultPos))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING IS UNIQUE}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE IS UNIQUE}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFD-5-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING IS KEY, age :: INT}), (c :City => {name :: STRING IS KEY}), (p)-[:LIVES_IN => {since :: DATE IS KEY}]->(c) }""",
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos)),
          propertyType("age", IntegerType(isNullable = true))
        ),
        nodeType(
          "City",
          "c",
          propertyType("name", StringType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING IS KEY}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING IS KEY}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE IS KEY}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`City` =>) REQUIRE (`n`.`name`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFD-5-2",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING IS KEY, age :: INT}),
        |  (c :City => {name :: STRING}) REQUIRE c.name IS KEY,
        |  (p)-[:LIVES_IN => {since :: DATE IS KEY}]->(c) }""".stripMargin,
      graphType(
        nodeType(
          "Person",
          "p",
          propertyType("name", StringType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos)),
          propertyType("age", IntegerType(isNullable = true))
        ),
        nodeTypeWithConstraints(
          "City",
          "c",
          Set(KeyConstraint(ArraySeq(prop("c", "name")))(defaultPos)),
          propertyType("name", StringType(isNullable = true))
        ),
        edgeType(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          nodeTypeRefByVar("c"),
          propertyType("since", DateType(isNullable = true), PropertyInlineKeyConstraint()(defaultPos))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}) REQUIRE (`c`.`name`) IS KEY,
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING IS KEY}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE IS KEY}]->(`c`)
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`City` =>) REQUIRE (`n`.`name`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFD-5-3",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}) REQUIRE p.name IS KEY,
        |  (c :City => {name :: STRING}) REQUIRE c.name IS KEY,
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c) REQUIRE l.since IS KEY }""".stripMargin,
      graphType(
        nodeTypeWithConstraints(
          "Person",
          "p",
          Set(KeyConstraint(ArraySeq(prop("p", "name")))(defaultPos)),
          propertyType("name", StringType(isNullable = true)),
          propertyType("age", IntegerType(isNullable = true))
        ),
        nodeTypeWithConstraints(
          "City",
          "c",
          Set(KeyConstraint(ArraySeq(prop("c", "name")))(defaultPos)),
          propertyType("name", StringType(isNullable = true))
        ),
        edgeTypeWithConstraints(
          nodeTypeRefByVar("p"),
          "LIVES_IN",
          "l",
          nodeTypeRefByVar("c"),
          Set(KeyConstraint(ArraySeq(prop("l", "since")))(defaultPos)),
          propertyType("since", DateType(isNullable = true))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}) REQUIRE (`c`.`name`) IS KEY,
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}) REQUIRE (`p`.`name`) IS KEY,
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`) REQUIRE (`l`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`City` =>) REQUIRE (`n`.`name`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFD-5-4",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR (p) REQUIRE p.name IS KEY,
        |  CONSTRAINT FOR (c) REQUIRE c.name IS KEY,
        |  CONSTRAINT FOR ()-[l]->()  REQUIRE l.since IS KEY }""".stripMargin,
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
          keyConstraint(nodeTypeRefByVar("p"), ArraySeq(prop("p", "name", defaultPos))),
          keyConstraint(nodeTypeRefByVar("c"), ArraySeq(prop("c", "name", defaultPos))),
          keyConstraint(edgeTypeRefByVar("l"), ArraySeq(prop("l", "since", defaultPos)))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`c`) REQUIRE (`c`.`name`) IS KEY,
        |  CONSTRAINT FOR (`p`) REQUIRE (`p`.`name`) IS KEY,
        |  CONSTRAINT FOR ()-[`l`]->() REQUIRE (`l`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`City` =>) REQUIRE (`n`.`name`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFD-5-5",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR (p :Person => ) REQUIRE p.name IS KEY,
        |  CONSTRAINT FOR (c :City => ) REQUIRE c.name IS KEY,
        |  CONSTRAINT FOR ()-[l:LIVES_IN => ]->()  REQUIRE l.since IS KEY }""".stripMargin,
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
          keyConstraint(identifyingNodeTypeRef("Person", "p"), ArraySeq(prop("p", "name", defaultPos))),
          keyConstraint(identifyingNodeTypeRef("City", "c"), ArraySeq(prop("c", "name", defaultPos))),
          keyConstraint(identifyingEdgeTypeRef("LIVES_IN", "l"), ArraySeq(prop("l", "since", defaultPos)))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`c`:`City` =>) REQUIRE (`c`.`name`) IS KEY,
        |  CONSTRAINT FOR (`p`:`Person` =>) REQUIRE (`p`.`name`) IS KEY,
        |  CONSTRAINT FOR ()-[`l`:`LIVES_IN` =>]->() REQUIRE (`l`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`City` =>) REQUIRE (`n`.`name`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-MCFD-5-6",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR (x :Person => ) REQUIRE x.name IS KEY,
        |  CONSTRAINT FOR (x :City => ) REQUIRE x.name IS KEY,
        |  CONSTRAINT FOR ()-[x:LIVES_IN => ]->()  REQUIRE x.since IS KEY }""".stripMargin,
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
          keyConstraint(identifyingNodeTypeRef("Person", "x"), ArraySeq(prop("x", "name", defaultPos))),
          keyConstraint(identifyingNodeTypeRef("City", "x"), ArraySeq(prop("x", "name", defaultPos))),
          keyConstraint(identifyingEdgeTypeRef("LIVES_IN", "x"), ArraySeq(prop("x", "since", defaultPos)))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`x`:`City` =>) REQUIRE (`x`.`name`) IS KEY,
        |  CONSTRAINT FOR (`x`:`Person` =>) REQUIRE (`x`.`name`) IS KEY,
        |  CONSTRAINT FOR ()-[`x`:`LIVES_IN` =>]->() REQUIRE (`x`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`City` =>) REQUIRE (`n`.`name`) IS KEY,
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY,
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCWN-5-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  CONSTRAINT myNameConstraint FOR (p) REQUIRE p.name IS KEY,
        |  CONSTRAINT myAgeConstraint FOR (p) REQUIRE p.age IS KEY,
        |  (c :City => {name :: STRING}),
        |  (p)-[:LIVES_IN => {since :: DATE}]->(c) }""".stripMargin,
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
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
          )
        ),
        Seq(
          keyConstraint("myNameConstraint", nodeTypeRefByVar("p"), ArraySeq(prop("p", "name", defaultPos))),
          keyConstraint("myAgeConstraint", nodeTypeRefByVar("p"), ArraySeq(prop("p", "age", defaultPos)))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT `myAgeConstraint` FOR (`p`) REQUIRE (`p`.`age`) IS KEY,
        |  CONSTRAINT `myNameConstraint` FOR (`p`) REQUIRE (`p`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT `myAgeConstraint` FOR (`n`:`Person` =>) REQUIRE (`n`.`age`) IS KEY,
        |  CONSTRAINT `myNameConstraint` FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCWN-6-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT myConstraint1 FOR (p) REQUIRE p.name IS KEY,
        |  CONSTRAINT myConstraint2 FOR (c) REQUIRE c.name IS KEY,
        |  CONSTRAINT myConstraint3 FOR ()-[l]->() REQUIRE l.since IS KEY }""".stripMargin,
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
          keyConstraint("myConstraint1", nodeTypeRefByVar("p"), ArraySeq(prop("p", "name", defaultPos))),
          keyConstraint("myConstraint2", nodeTypeRefByVar("c"), ArraySeq(prop("c", "name", defaultPos))),
          keyConstraint("myConstraint3", edgeTypeRefByVar("l"), ArraySeq(prop("l", "since", defaultPos)))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT `myConstraint2` FOR (`c`) REQUIRE (`c`.`name`) IS KEY,
        |  CONSTRAINT `myConstraint1` FOR (`p`) REQUIRE (`p`.`name`) IS KEY,
        |  CONSTRAINT `myConstraint3` FOR ()-[`l`]->() REQUIRE (`l`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT `myConstraint2` FOR (`n`:`City` =>) REQUIRE (`n`.`name`) IS KEY,
        |  CONSTRAINT `myConstraint1` FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY,
        |  CONSTRAINT `myConstraint3` FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCWAS-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR (c: Person =>) REQUIRE c.name IS KEY }""".stripMargin,
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
          keyConstraint(identifyingNodeTypeRef("Person", "c"), ArraySeq(prop("c", "name", defaultPos)))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`c`:`Person` =>) REQUIRE (`c`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCWAS-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |CONSTRAINT FOR (c: Person =>) REQUIRE c.name IS UNIQUE }""".stripMargin,
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
          uniquenessConstraint(identifyingNodeTypeRef("Person", "c"), ArraySeq(prop("c", "name", defaultPos)))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`c`:`Person` =>) REQUIRE (`c`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCWAS-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR (l: Person =>) REQUIRE l.name IS KEY }""".stripMargin,
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
          keyConstraint(identifyingNodeTypeRef("Person", "l"), ArraySeq(prop("l", "name", defaultPos)))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`l`:`Person` =>) REQUIRE (`l`.`name`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCWAS-4-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}), (p)-[l: LIVES_IN => {since :: DATE}]->(c), CONSTRAINT FOR (l: Person =>) REQUIRE l.name IS UNIQUE }""",
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
          uniquenessConstraint(identifyingNodeTypeRef("Person", "l"), ArraySeq(prop("l", "name", defaultPos)))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR (`l`:`Person` =>) REQUIRE (`l`.`name`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR (`n`:`Person` =>) REQUIRE (`n`.`name`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCWAS-5-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |(p)-[k: KNOWS => {since :: DATE}]->(c), (p)-[l: LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR ()-[k: LIVES_IN =>]->() REQUIRE k.since IS KEY }""".stripMargin,
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
            "KNOWS",
            "k",
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
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
          keyConstraint(identifyingEdgeTypeRef("LIVES_IN", "k"), ArraySeq(prop("k", "since", defaultPos)))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`k`:`KNOWS` => {`since` :: DATE}]->(`c`),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`k`:`LIVES_IN` =>]->() REQUIRE (`k`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`KNOWS` => {`since` :: DATE}]->(:`City` =>),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCWAS-6-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |(p)-[k: KNOWS => {since :: DATE}]->(c), (p)-[l: LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR ()-[k: LIVES_IN =>]->() REQUIRE k.since IS UNIQUE }""".stripMargin,
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
            "KNOWS",
            "k",
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
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
          uniquenessConstraint(
            identifyingEdgeTypeRef("LIVES_IN", "k"),
            ArraySeq(prop("k", "since", defaultPos))
          )
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`k`:`KNOWS` => {`since` :: DATE}]->(`c`),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`k`:`LIVES_IN` =>]->() REQUIRE (`k`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`KNOWS` => {`since` :: DATE}]->(:`City` =>),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCWAS-7-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}), (p)-[k: KNOWS => {since :: DATE}]->(c), (p)-[l: LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR ()-[p: LIVES_IN =>]->() REQUIRE p.since IS KEY }""".stripMargin,
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
            "KNOWS",
            "k",
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
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
          keyConstraint(
            identifyingEdgeTypeRef("LIVES_IN", "p"),
            ArraySeq(prop("p", "since", defaultPos))
          )
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`k`:`KNOWS` => {`since` :: DATE}]->(`c`),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`p`:`LIVES_IN` =>]->() REQUIRE (`p`.`since`) IS KEY
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`KNOWS` => {`since` :: DATE}]->(:`City` =>),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS KEY
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "UDC-PE-SCWAS-8-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[k: KNOWS => {since :: DATE}]->(c), (p)-[l: LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR ()-[p: LIVES_IN =>]->() REQUIRE p.since IS UNIQUE }""".stripMargin,
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
            "KNOWS",
            "k",
            nodeTypeRefByVar("c"),
            propertyType("since", DateType(isNullable = true))
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
          uniquenessConstraint(
            identifyingEdgeTypeRef("LIVES_IN", "p"),
            ArraySeq(prop("p", "since", defaultPos))
          )
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`k`:`KNOWS` => {`since` :: DATE}]->(`c`),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`p`:`LIVES_IN` =>]->() REQUIRE (`p`.`since`) IS UNIQUE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`KNOWS` => {`since` :: DATE}]->(:`City` =>),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN` =>]->() REQUIRE (`r`.`since`) IS UNIQUE
        |}""".stripMargin
    )
  )

  private def idc(): Seq[GraphTypeTestCase] = Seq(
    GraphTypeTestCase(
      "IDC-PE-SCNIL-1-1",
      """ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR (s :Student) REQUIRE s.studId IS NOT NULL }""",
      graphType(
        List.empty,
        List(existsConstraint(nodeTypeRefByLabel("Student", "s"), ArraySeq(prop("s", "studId"))))
      ),
      """{
        |  CONSTRAINT FOR (`s`:`Student`) REQUIRE (`s`.`studId`) IS NOT NULL
        |}""".stripMargin,
      """{
        |  CONSTRAINT FOR (`n`:`Student`) REQUIRE (`n`.`studId`) IS NOT NULL
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "IDC-PE-SCNIL-2-1",
      """ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR (s :Student) REQUIRE s.studId :: INT }""",
      graphType(
        List.empty,
        List(propertyTypeConstraint(
          nodeTypeRefByLabel("Student", "s"),
          ArraySeq(prop("s", "studId")),
          IntegerType(isNullable = true)(defaultPos)
        ))
      ),
      """{
        |  CONSTRAINT FOR (`s`:`Student`) REQUIRE (`s`.`studId`) IS :: INTEGER
        |}""".stripMargin,
      """{
        |  CONSTRAINT FOR (`n`:`Student`) REQUIRE (`n`.`studId`) IS :: INTEGER
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "IDC-PE-SCNIL-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  CONSTRAINT FOR (s :Student) REQUIRE s.studId IS NOT NULL }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          )
        ),
        List(existsConstraint(nodeTypeRefByLabel("Student", "s"), ArraySeq(prop("s", "studId"))))
      ),
      """{
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`s`:`Student`) REQUIRE (`s`.`studId`) IS NOT NULL
        |}""".stripMargin,
      """{
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Student`) REQUIRE (`n`.`studId`) IS NOT NULL
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "IDC-PE-SCNIL-4-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  CONSTRAINT FOR (s :Student) REQUIRE s.studId :: INT }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          )
        ),
        List(propertyTypeConstraint(
          nodeTypeRefByLabel("Student", "s"),
          ArraySeq(prop("s", "studId")),
          IntegerType(isNullable = true)(defaultPos)
        ))
      ),
      """{
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`s`:`Student`) REQUIRE (`s`.`studId`) IS :: INTEGER
        |}""".stripMargin,
      """{
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Student`) REQUIRE (`n`.`studId`) IS :: INTEGER
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "IDC-PE-SCNIL-5-1",
      """ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR ()-[l:LIVES_IN]->() REQUIRE l.since IS NOT NULL }""".stripMargin,
      graphType(
        List.empty,
        List(existsConstraint(edgeTypeRefByLabel("LIVES_IN", "l"), ArraySeq(prop("l", "since"))))
      ),
      """{
        |  CONSTRAINT FOR ()-[`l`:`LIVES_IN`]->() REQUIRE (`l`.`since`) IS NOT NULL
        |}""".stripMargin,
      """{
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN`]->() REQUIRE (`r`.`since`) IS NOT NULL
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "IDC-PE-SCNIL-6-1",
      """ALTER CURRENT GRAPH TYPE SET { CONSTRAINT FOR ()-[l:LIVES_IN]->() REQUIRE l.since :: DATE }""",
      graphType(
        List.empty,
        List(propertyTypeConstraint(
          edgeTypeRefByLabel("LIVES_IN", "l"),
          ArraySeq(prop("l", "since")),
          DateType(isNullable = true)(defaultPos)
        ))
      ),
      """{
        |  CONSTRAINT FOR ()-[`l`:`LIVES_IN`]->() REQUIRE (`l`.`since`) IS :: DATE
        |}""".stripMargin,
      """{
        |  CONSTRAINT FOR ()-[`r`:`LIVES_IN`]->() REQUIRE (`r`.`since`) IS :: DATE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "IDC-PE-SCNIL-7-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR ()-[k:KNOWS]->() REQUIRE k.since IS NOT NULL }""".stripMargin,
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
          existsConstraint(edgeTypeRefByLabel("KNOWS", "k"), ArraySeq(prop("k", "since", defaultPos)))
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`k`:`KNOWS`]->() REQUIRE (`k`.`since`) IS NOT NULL
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`KNOWS`]->() REQUIRE (`r`.`since`) IS NOT NULL
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "IDC-PE-SCNIL-8-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR ()-[k:KNOWS]->() REQUIRE k.since :: DATE }""".stripMargin,
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
          propertyTypeConstraint(
            edgeTypeRefByLabel("KNOWS", "k"),
            ArraySeq(prop("k", "since", defaultPos)),
            DateType(isNullable = true)(defaultPos)
          )
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`k`:`KNOWS`]->() REQUIRE (`k`.`since`) IS :: DATE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`KNOWS`]->() REQUIRE (`r`.`since`) IS :: DATE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "IDC-PE-SCIL-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => :Taxpayer), (c :Company => :Taxpayer), CONSTRAINT FOR (s :Taxpayer) REQUIRE s.taxId :: STRING }""",
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            Set("Taxpayer")
          ),
          nodeType(
            "Company",
            "c",
            Set("Taxpayer")
          )
        ),
        Seq(
          propertyTypeConstraint(
            nodeTypeRefByLabel("Taxpayer", "s"),
            ArraySeq(prop("s", "taxId", defaultPos)),
            StringType(isNullable = true)(defaultPos)
          )
        )
      ),
      """{
        |  (`c`:`Company` => :`Taxpayer`),
        |  (`p`:`Person` => :`Taxpayer`),
        |  CONSTRAINT FOR (`s`:`Taxpayer`) REQUIRE (`s`.`taxId`) IS :: STRING
        |}""".stripMargin,
      """{
        |  (:`Company` => :`Taxpayer`),
        |  (:`Person` => :`Taxpayer`),
        |  CONSTRAINT FOR (`n`:`Taxpayer`) REQUIRE (`n`.`taxId`) IS :: STRING
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "IDC-PE-MCSNIL-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  CONSTRAINT FOR (s :Student) REQUIRE s.studId IS NOT NULL,
        |  CONSTRAINT FOR (s :Student) REQUIRE s.studId :: INT }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          )
        ),
        Seq(
          existsConstraint(
            nodeTypeRefByLabel("Student", "s"),
            ArraySeq(prop("s", "studId", defaultPos))
          ),
          propertyTypeConstraint(
            nodeTypeRefByLabel("Student", "s"),
            ArraySeq(prop("s", "studId", defaultPos)),
            IntegerType(isNullable = true)(defaultPos)
          )
        )
      ),
      """{
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`s`:`Student`) REQUIRE (`s`.`studId`) IS NOT NULL,
        |  CONSTRAINT FOR (`s`:`Student`) REQUIRE (`s`.`studId`) IS :: INTEGER
        |}""".stripMargin,
      """{
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Student`) REQUIRE (`n`.`studId`) IS NOT NULL,
        |  CONSTRAINT FOR (`n`:`Student`) REQUIRE (`n`.`studId`) IS :: INTEGER
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "IDC-PE-MCSNIL-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  CONSTRAINT FOR (s :Student) REQUIRE s.studId IS NOT NULL,
        |  CONSTRAINT FOR (s :Student) REQUIRE s.studId :: INT,
        |  CONSTRAINT FOR (s :Student) REQUIRE s.name IS NOT NULL,
        |CONSTRAINT FOR (s :Student) REQUIRE s.name :: STRING }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          )
        ),
        Seq(
          existsConstraint(
            nodeTypeRefByLabel("Student", "s"),
            ArraySeq(prop("s", "studId", defaultPos))
          ),
          propertyTypeConstraint(
            nodeTypeRefByLabel("Student", "s"),
            ArraySeq(prop("s", "studId", defaultPos)),
            IntegerType(isNullable = true)(defaultPos)
          ),
          existsConstraint(
            nodeTypeRefByLabel("Student", "s"),
            ArraySeq(prop("s", "name", defaultPos))
          ),
          propertyTypeConstraint(
            nodeTypeRefByLabel("Student", "s"),
            ArraySeq(prop("s", "name", defaultPos)),
            StringType(isNullable = true)(defaultPos)
          )
        )
      ),
      """{
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`s`:`Student`) REQUIRE (`s`.`name`) IS NOT NULL,
        |  CONSTRAINT FOR (`s`:`Student`) REQUIRE (`s`.`name`) IS :: STRING,
        |  CONSTRAINT FOR (`s`:`Student`) REQUIRE (`s`.`studId`) IS NOT NULL,
        |  CONSTRAINT FOR (`s`:`Student`) REQUIRE (`s`.`studId`) IS :: INTEGER
        |}""".stripMargin,
      """{
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Student`) REQUIRE (`n`.`name`) IS NOT NULL,
        |  CONSTRAINT FOR (`n`:`Student`) REQUIRE (`n`.`name`) IS :: STRING,
        |  CONSTRAINT FOR (`n`:`Student`) REQUIRE (`n`.`studId`) IS NOT NULL,
        |  CONSTRAINT FOR (`n`:`Student`) REQUIRE (`n`.`studId`) IS :: INTEGER
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "IDC-PE-MCSNIL-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR ()-[k:KNOWS]->() REQUIRE k.since IS NOT NULL,
        |  CONSTRAINT FOR ()-[k:KNOWS]->() REQUIRE k.since :: DATE }""".stripMargin,
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
            edgeTypeRefByLabel("KNOWS", "k"),
            ArraySeq(prop("k", "since", defaultPos))
          ),
          propertyTypeConstraint(
            edgeTypeRefByLabel("KNOWS", "k"),
            ArraySeq(prop("k", "since", defaultPos)),
            DateType(isNullable = true)(defaultPos)
          )
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`k`:`KNOWS`]->() REQUIRE (`k`.`since`) IS NOT NULL,
        |  CONSTRAINT FOR ()-[`k`:`KNOWS`]->() REQUIRE (`k`.`since`) IS :: DATE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`KNOWS`]->() REQUIRE (`r`.`since`) IS NOT NULL,
        |  CONSTRAINT FOR ()-[`r`:`KNOWS`]->() REQUIRE (`r`.`since`) IS :: DATE
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "IDC-PE-MCSIL-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => :Taxpayer {name :: STRING, age :: INT}), (c :Company => :Taxpayer {name :: STRING, isin :: STRING}),
        |  CONSTRAINT FOR (s :Taxpayer) REQUIRE s.taxId IS NOT NULL, CONSTRAINT FOR (s :Taxpayer) REQUIRE s.taxRate :: FLOAT }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            Set("Taxpayer"),
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          ),
          nodeType(
            "Company",
            "c",
            Set("Taxpayer"),
            propertyType("name", StringType(isNullable = true)),
            propertyType("isin", StringType(isNullable = true))
          )
        ),
        Seq(
          existsConstraint(
            nodeTypeRefByLabel("Taxpayer", "s"),
            ArraySeq(prop("s", "taxId", defaultPos))
          ),
          propertyTypeConstraint(
            nodeTypeRefByLabel("Taxpayer", "s"),
            ArraySeq(prop("s", "taxRate", defaultPos)),
            FloatType(isNullable = true)(defaultPos)
          )
        )
      ),
      """{
        |  (`c`:`Company` => :`Taxpayer` {`isin` :: STRING, `name` :: STRING}),
        |  (`p`:`Person` => :`Taxpayer` {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`s`:`Taxpayer`) REQUIRE (`s`.`taxId`) IS NOT NULL,
        |  CONSTRAINT FOR (`s`:`Taxpayer`) REQUIRE (`s`.`taxRate`) IS :: FLOAT
        |}""".stripMargin,
      """{
        |  (:`Company` => :`Taxpayer` {`isin` :: STRING, `name` :: STRING}),
        |  (:`Person` => :`Taxpayer` {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Taxpayer`) REQUIRE (`n`.`taxId`) IS NOT NULL,
        |  CONSTRAINT FOR (`n`:`Taxpayer`) REQUIRE (`n`.`taxRate`) IS :: FLOAT
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "IDC-PE-MCSIL-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => :Taxpayer {name :: STRING, age :: INT}),
        |  (c :Company => :Taxpayer {name :: STRING, isin :: STRING}),
        |  CONSTRAINT FOR (s :Taxpayer) REQUIRE s.taxId IS NOT NULL, CONSTRAINT FOR (s :Taxpayer) REQUIRE s.taxId :: STRING,
        |  CONSTRAINT FOR (s :Taxpayer) REQUIRE s.taxRate IS NOT NULL, CONSTRAINT FOR (s :Taxpayer) REQUIRE s.taxRate :: FLOAT }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            Set("Taxpayer"),
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          ),
          nodeType(
            "Company",
            "c",
            Set("Taxpayer"),
            propertyType("name", StringType(isNullable = true)),
            propertyType("isin", StringType(isNullable = true))
          )
        ),
        Seq(
          existsConstraint(
            nodeTypeRefByLabel("Taxpayer", "s"),
            ArraySeq(prop("s", "taxId", defaultPos))
          ),
          propertyTypeConstraint(
            nodeTypeRefByLabel("Taxpayer", "s"),
            ArraySeq(prop("s", "taxId", defaultPos)),
            StringType(isNullable = true)(defaultPos)
          ),
          existsConstraint(
            nodeTypeRefByLabel("Taxpayer", "s"),
            ArraySeq(prop("s", "taxRate", defaultPos))
          ),
          propertyTypeConstraint(
            nodeTypeRefByLabel("Taxpayer", "s"),
            ArraySeq(prop("s", "taxRate", defaultPos)),
            FloatType(isNullable = true)(defaultPos)
          )
        )
      ),
      """{
        |  (`c`:`Company` => :`Taxpayer` {`isin` :: STRING, `name` :: STRING}),
        |  (`p`:`Person` => :`Taxpayer` {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`s`:`Taxpayer`) REQUIRE (`s`.`taxId`) IS NOT NULL,
        |  CONSTRAINT FOR (`s`:`Taxpayer`) REQUIRE (`s`.`taxId`) IS :: STRING,
        |  CONSTRAINT FOR (`s`:`Taxpayer`) REQUIRE (`s`.`taxRate`) IS NOT NULL,
        |  CONSTRAINT FOR (`s`:`Taxpayer`) REQUIRE (`s`.`taxRate`) IS :: FLOAT
        |}""".stripMargin,
      """{
        |  (:`Company` => :`Taxpayer` {`isin` :: STRING, `name` :: STRING}),
        |  (:`Person` => :`Taxpayer` {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Taxpayer`) REQUIRE (`n`.`taxId`) IS NOT NULL,
        |  CONSTRAINT FOR (`n`:`Taxpayer`) REQUIRE (`n`.`taxId`) IS :: STRING,
        |  CONSTRAINT FOR (`n`:`Taxpayer`) REQUIRE (`n`.`taxRate`) IS NOT NULL,
        |  CONSTRAINT FOR (`n`:`Taxpayer`) REQUIRE (`n`.`taxRate`) IS :: FLOAT
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "IDC-PE-MCDNIL-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), CONSTRAINT FOR (s :Student) REQUIRE s.studId IS NOT NULL, CONSTRAINT FOR (s :Happy) REQUIRE s.degree :: FLOAT }""",
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          )
        ),
        Seq(
          existsConstraint(
            nodeTypeRefByLabel("Student", "s"),
            ArraySeq(prop("s", "studId", defaultPos))
          ),
          propertyTypeConstraint(
            nodeTypeRefByLabel("Happy", "s"),
            ArraySeq(prop("s", "degree", defaultPos)),
            FloatType(isNullable = true)(defaultPos)
          )
        )
      ),
      """{
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`s`:`Happy`) REQUIRE (`s`.`degree`) IS :: FLOAT,
        |  CONSTRAINT FOR (`s`:`Student`) REQUIRE (`s`.`studId`) IS NOT NULL
        |}""".stripMargin,
      """{
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Happy`) REQUIRE (`n`.`degree`) IS :: FLOAT,
        |  CONSTRAINT FOR (`n`:`Student`) REQUIRE (`n`.`studId`) IS NOT NULL
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "IDC-PE-MCDNIL-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}),
        |  CONSTRAINT FOR (s :Student) REQUIRE s.studId IS NOT NULL, CONSTRAINT FOR (s :Student) REQUIRE s.studId :: INT,
        |  CONSTRAINT FOR (s :Happy) REQUIRE s.degree IS NOT NULL, CONSTRAINT FOR (s :Happy) REQUIRE s.degree :: FLOAT }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          )
        ),
        Seq(
          existsConstraint(
            nodeTypeRefByLabel("Student", "s"),
            ArraySeq(prop("s", "studId", defaultPos))
          ),
          propertyTypeConstraint(
            nodeTypeRefByLabel("Student", "s"),
            ArraySeq(prop("s", "studId", defaultPos)),
            IntegerType(isNullable = true)(defaultPos)
          ),
          existsConstraint(
            nodeTypeRefByLabel("Happy", "s"),
            ArraySeq(prop("s", "degree", defaultPos))
          ),
          propertyTypeConstraint(
            nodeTypeRefByLabel("Happy", "s"),
            ArraySeq(prop("s", "degree", defaultPos)),
            FloatType(isNullable = true)(defaultPos)
          )
        )
      ),
      """{
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`s`:`Happy`) REQUIRE (`s`.`degree`) IS NOT NULL,
        |  CONSTRAINT FOR (`s`:`Happy`) REQUIRE (`s`.`degree`) IS :: FLOAT,
        |  CONSTRAINT FOR (`s`:`Student`) REQUIRE (`s`.`studId`) IS NOT NULL,
        |  CONSTRAINT FOR (`s`:`Student`) REQUIRE (`s`.`studId`) IS :: INTEGER
        |}""".stripMargin,
      """{
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Happy`) REQUIRE (`n`.`degree`) IS NOT NULL,
        |  CONSTRAINT FOR (`n`:`Happy`) REQUIRE (`n`.`degree`) IS :: FLOAT,
        |  CONSTRAINT FOR (`n`:`Student`) REQUIRE (`n`.`studId`) IS NOT NULL,
        |  CONSTRAINT FOR (`n`:`Student`) REQUIRE (`n`.`studId`) IS :: INTEGER
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "IDC-PE-MCDNIL-3-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR ()-[k:KNOWS]->() REQUIRE k.since IS NOT NULL, CONSTRAINT FOR ()-[r:RELATED]->() REQUIRE r.cause :: STRING }""".stripMargin,
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
            edgeTypeRefByLabel("KNOWS", "k"),
            ArraySeq(prop("k", "since", defaultPos))
          ),
          propertyTypeConstraint(
            edgeTypeRefByLabel("RELATED", "r"),
            ArraySeq(prop("r", "cause", defaultPos)),
            StringType(isNullable = true)(defaultPos)
          )
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`k`:`KNOWS`]->() REQUIRE (`k`.`since`) IS NOT NULL,
        |  CONSTRAINT FOR ()-[`r`:`RELATED`]->() REQUIRE (`r`.`cause`) IS :: STRING
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`KNOWS`]->() REQUIRE (`r`.`since`) IS NOT NULL,
        |  CONSTRAINT FOR ()-[`r`:`RELATED`]->() REQUIRE (`r`.`cause`) IS :: STRING
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "IDC-PE-MCDNIL-4-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}), (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT FOR ()-[k:KNOWS]->() REQUIRE k.since IS NOT NULL, CONSTRAINT FOR ()-[k:KNOWS]->() REQUIRE k.since :: DATE,
        |  CONSTRAINT FOR ()-[r:RELATED]->() REQUIRE r.cause :: STRING, CONSTRAINT FOR ()-[r:RELATED]->() REQUIRE r.cause IS NOT NULL }""".stripMargin,
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
            edgeTypeRefByLabel("KNOWS", "k"),
            ArraySeq(prop("k", "since", defaultPos))
          ),
          propertyTypeConstraint(
            edgeTypeRefByLabel("KNOWS", "k"),
            ArraySeq(prop("k", "since", defaultPos)),
            DateType(isNullable = true)(defaultPos)
          ),
          propertyTypeConstraint(
            edgeTypeRefByLabel("RELATED", "r"),
            ArraySeq(prop("r", "cause", defaultPos)),
            StringType(isNullable = true)(defaultPos)
          ),
          existsConstraint(
            edgeTypeRefByLabel("RELATED", "r"),
            ArraySeq(prop("r", "cause", defaultPos))
          )
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT FOR ()-[`k`:`KNOWS`]->() REQUIRE (`k`.`since`) IS NOT NULL,
        |  CONSTRAINT FOR ()-[`k`:`KNOWS`]->() REQUIRE (`k`.`since`) IS :: DATE,
        |  CONSTRAINT FOR ()-[`r`:`RELATED`]->() REQUIRE (`r`.`cause`) IS NOT NULL,
        |  CONSTRAINT FOR ()-[`r`:`RELATED`]->() REQUIRE (`r`.`cause`) IS :: STRING
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT FOR ()-[`r`:`KNOWS`]->() REQUIRE (`r`.`since`) IS NOT NULL,
        |  CONSTRAINT FOR ()-[`r`:`KNOWS`]->() REQUIRE (`r`.`since`) IS :: DATE,
        |  CONSTRAINT FOR ()-[`r`:`RELATED`]->() REQUIRE (`r`.`cause`) IS NOT NULL,
        |  CONSTRAINT FOR ()-[`r`:`RELATED`]->() REQUIRE (`r`.`cause`) IS :: STRING
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "IDC-PE-MCDIL-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => :Taxpayer&Named {name :: STRING, age :: INT}), (c :Company => :Taxpayer {name :: STRING, isin :: STRING}),
        |  (t :Town => :Named {name :: STRING, isin :: STRING}),
        |  CONSTRAINT FOR (s :Taxpayer) REQUIRE s.taxId IS NOT NULL, CONSTRAINT FOR (n :Named) REQUIRE n.name :: STRING }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            Set("Taxpayer", "Named"),
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          ),
          nodeType(
            "Company",
            "c",
            Set("Taxpayer"),
            propertyType("name", StringType(isNullable = true)),
            propertyType("isin", StringType(isNullable = true))
          ),
          nodeType(
            "Town",
            "t",
            Set("Named"),
            propertyType("name", StringType(isNullable = true)),
            propertyType("isin", StringType(isNullable = true))
          )
        ),
        Seq(
          existsConstraint(
            nodeTypeRefByLabel("Taxpayer", "s"),
            ArraySeq(prop("s", "taxId", defaultPos))
          ),
          propertyTypeConstraint(
            nodeTypeRefByLabel("Named", "n"),
            ArraySeq(prop("n", "name", defaultPos)),
            StringType(isNullable = true)(defaultPos)
          )
        )
      ),
      """{
        |  (`c`:`Company` => :`Taxpayer` {`isin` :: STRING, `name` :: STRING}),
        |  (`p`:`Person` => :`Named`&`Taxpayer` {`age` :: INTEGER, `name` :: STRING}),
        |  (`t`:`Town` => :`Named` {`isin` :: STRING, `name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Named`) REQUIRE (`n`.`name`) IS :: STRING,
        |  CONSTRAINT FOR (`s`:`Taxpayer`) REQUIRE (`s`.`taxId`) IS NOT NULL
        |}""".stripMargin,
      """{
        |  (:`Company` => :`Taxpayer` {`isin` :: STRING, `name` :: STRING}),
        |  (:`Person` => :`Named`&`Taxpayer` {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Town` => :`Named` {`isin` :: STRING, `name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Named`) REQUIRE (`n`.`name`) IS :: STRING,
        |  CONSTRAINT FOR (`n`:`Taxpayer`) REQUIRE (`n`.`taxId`) IS NOT NULL
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "IDC-PE-MCDIL-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => :Taxpayer&Named {name :: STRING, age :: INT}), (c :Company => :Taxpayer {name :: STRING, isin :: STRING}),
        |  (t :Town => :Named {name :: STRING, isin :: STRING}),
        |  CONSTRAINT FOR (s :Taxpayer) REQUIRE s.taxId IS NOT NULL,CONSTRAINT FOR (s :Taxpayer) REQUIRE s.taxId :: STRING,
        |  CONSTRAINT FOR (n :Named) REQUIRE n.name IS NOT NULL,CONSTRAINT FOR (n :Named) REQUIRE n.name :: STRING }""".stripMargin,
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            Set("Taxpayer", "Named"),
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          ),
          nodeType(
            "Company",
            "c",
            Set("Taxpayer"),
            propertyType("name", StringType(isNullable = true)),
            propertyType("isin", StringType(isNullable = true))
          ),
          nodeType(
            "Town",
            "t",
            Set("Named"),
            propertyType("name", StringType(isNullable = true)),
            propertyType("isin", StringType(isNullable = true))
          )
        ),
        Seq(
          existsConstraint(
            nodeTypeRefByLabel("Taxpayer", "s"),
            ArraySeq(prop("s", "taxId", defaultPos))
          ),
          propertyTypeConstraint(
            nodeTypeRefByLabel("Taxpayer", "s"),
            ArraySeq(prop("s", "taxId", defaultPos)),
            StringType(isNullable = true)(defaultPos)
          ),
          existsConstraint(
            nodeTypeRefByLabel("Named", "n"),
            ArraySeq(prop("n", "name", defaultPos))
          ),
          propertyTypeConstraint(
            nodeTypeRefByLabel("Named", "n"),
            ArraySeq(prop("n", "name", defaultPos)),
            StringType(isNullable = true)(defaultPos)
          )
        )
      ),
      """{
        |  (`c`:`Company` => :`Taxpayer` {`isin` :: STRING, `name` :: STRING}),
        |  (`p`:`Person` => :`Named`&`Taxpayer` {`age` :: INTEGER, `name` :: STRING}),
        |  (`t`:`Town` => :`Named` {`isin` :: STRING, `name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Named`) REQUIRE (`n`.`name`) IS NOT NULL,
        |  CONSTRAINT FOR (`n`:`Named`) REQUIRE (`n`.`name`) IS :: STRING,
        |  CONSTRAINT FOR (`s`:`Taxpayer`) REQUIRE (`s`.`taxId`) IS NOT NULL,
        |  CONSTRAINT FOR (`s`:`Taxpayer`) REQUIRE (`s`.`taxId`) IS :: STRING
        |}""".stripMargin,
      """{
        |  (:`Company` => :`Taxpayer` {`isin` :: STRING, `name` :: STRING}),
        |  (:`Person` => :`Named`&`Taxpayer` {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Town` => :`Named` {`isin` :: STRING, `name` :: STRING}),
        |  CONSTRAINT FOR (`n`:`Named`) REQUIRE (`n`.`name`) IS NOT NULL,
        |  CONSTRAINT FOR (`n`:`Named`) REQUIRE (`n`.`name`) IS :: STRING,
        |  CONSTRAINT FOR (`n`:`Taxpayer`) REQUIRE (`n`.`taxId`) IS NOT NULL,
        |  CONSTRAINT FOR (`n`:`Taxpayer`) REQUIRE (`n`.`taxId`) IS :: STRING
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "IDC-PE-MCWN-1-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), CONSTRAINT myExistConstraint FOR (s :Student) REQUIRE s.studId IS NOT NULL, CONSTRAINT myTypeConstraint FOR (s :Student) REQUIRE s.studId :: INT }""",
      graphType(
        Seq(
          nodeType(
            "Person",
            "p",
            propertyType("name", StringType(isNullable = true)),
            propertyType("age", IntegerType(isNullable = true))
          )
        ),
        Seq(
          existsConstraint(
            "myExistConstraint",
            nodeTypeRefByLabel("Student", "s"),
            ArraySeq(prop("s", "studId", defaultPos))
          ),
          propertyTypeConstraint(
            "myTypeConstraint",
            nodeTypeRefByLabel("Student", "s"),
            ArraySeq(prop("s", "studId", defaultPos)),
            IntegerType(isNullable = true)(defaultPos)
          )
        )
      ),
      """{
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT `myExistConstraint` FOR (`s`:`Student`) REQUIRE (`s`.`studId`) IS NOT NULL,
        |  CONSTRAINT `myTypeConstraint` FOR (`s`:`Student`) REQUIRE (`s`.`studId`) IS :: INTEGER
        |}""".stripMargin,
      """{
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  CONSTRAINT `myExistConstraint` FOR (`n`:`Student`) REQUIRE (`n`.`studId`) IS NOT NULL,
        |  CONSTRAINT `myTypeConstraint` FOR (`n`:`Student`) REQUIRE (`n`.`studId`) IS :: INTEGER
        |}""".stripMargin
    ),
    GraphTypeTestCase(
      "IDC-PE-MCWN-2-1",
      """ALTER CURRENT GRAPH TYPE SET { (p :Person => {name :: STRING, age :: INT}), (c :City => {name :: STRING}),
        |  (p)-[l:LIVES_IN => {since :: DATE}]->(c),
        |  CONSTRAINT myExistConstraint FOR ()-[k:KNOWS]->() REQUIRE k.since IS NOT NULL, CONSTRAINT myTypeConstraint FOR ()-[k:KNOWS]->() REQUIRE k.since :: DATE }""".stripMargin,
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
            "myExistConstraint",
            edgeTypeRefByLabel("KNOWS", "k"),
            ArraySeq(prop("k", "since", defaultPos))
          ),
          propertyTypeConstraint(
            "myTypeConstraint",
            edgeTypeRefByLabel("KNOWS", "k"),
            ArraySeq(prop("k", "since", defaultPos)),
            DateType(isNullable = true)(defaultPos)
          )
        )
      ),
      """{
        |  (`c`:`City` => {`name` :: STRING}),
        |  (`p`:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (`p`)-[`l`:`LIVES_IN` => {`since` :: DATE}]->(`c`),
        |  CONSTRAINT `myExistConstraint` FOR ()-[`k`:`KNOWS`]->() REQUIRE (`k`.`since`) IS NOT NULL,
        |  CONSTRAINT `myTypeConstraint` FOR ()-[`k`:`KNOWS`]->() REQUIRE (`k`.`since`) IS :: DATE
        |}""".stripMargin,
      """{
        |  (:`City` => {`name` :: STRING}),
        |  (:`Person` => {`age` :: INTEGER, `name` :: STRING}),
        |  (:`Person` =>)-[:`LIVES_IN` => {`since` :: DATE}]->(:`City` =>),
        |  CONSTRAINT `myExistConstraint` FOR ()-[`r`:`KNOWS`]->() REQUIRE (`r`.`since`) IS NOT NULL,
        |  CONSTRAINT `myTypeConstraint` FOR ()-[`r`:`KNOWS`]->() REQUIRE (`r`.`since`) IS :: DATE
        |}""".stripMargin
    )
  )

  val testcases: Seq[GraphTypeTestCase] = re() ++ snt() ++ mnt() ++ set() ++ met() ++ udc() ++ idc()
}
