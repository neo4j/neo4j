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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.AnyType
import org.neo4j.cypher.internal.util.symbols.BooleanType
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.DateType
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.Integer16Type
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.symbols.VectorType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe

import scala.collection.immutable.ArraySeq

class GraphTypeUnitTest extends CypherFunSuite {
  implicit val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

  private val pos: InputPosition.Range = InputPosition.NONE
  private def labelName(label: String): LabelName = LabelName(label)(pos)
  private def relTypeName(relType: String): RelTypeName = RelTypeName(relType)(pos)
  private def propKeyName(prop: String): PropertyKeyName = PropertyKeyName(prop)(pos)

  // Graph type info for SET/ADD/ALTER

  test("create single node elem type with implied label - forShowOutput=false") {
    // GIVEN
    val nodeElemType = NodeElementType(
      labelName("Label"),
      Set(labelName("Label2")),
      Set.empty
    )

    // WHEN
    val result = GraphType.graphTypeInfoForPlan(Set(nodeElemType), Set.empty)

    // THEN
    result should equal(
      "{ (:Label => :Label2) }"
    )
  }

  test("create single node elem type with properties - forShowOutput=false") {
    // GIVEN
    val nodeElemType = NodeElementType(
      labelName("Label"),
      Set.empty,
      Set(
        PropertyType(propKeyName("prop3"), IntegerType(isNullable = false)(pos)),
        PropertyType(propKeyName("prop2"), IntegerType(isNullable = true)(pos)),
        PropertyType(propKeyName("prop1"), AnyType(isNullable = false)(pos))
      )
    )

    // WHEN
    val result = GraphType.graphTypeInfoForPlan(Set(nodeElemType), Set.empty)

    // THEN
    result should equal(
      "{ (:Label => {prop1 :: ANY NOT NULL, prop2 :: INTEGER, prop3 :: INTEGER NOT NULL}) }"
    )
  }

  test("create single node elem type with full type - forShowOutput=false") {
    // GIVEN
    val nodeElemType = NodeElementType(
      labelName("Label"),
      Set(labelName("Label3"), labelName("Label2")),
      Set(PropertyType(propKeyName("prop"), IntegerType(isNullable = false)(pos)))
    )

    // WHEN
    val result = GraphType.graphTypeInfoForPlan(Set(nodeElemType), Set.empty)

    // THEN
    result should equal(
      "{ (:Label => :Label2&Label3 {prop :: INTEGER NOT NULL}) }"
    )
  }

  test("create single rel elem type with source and target label - forShowOutput=false") {
    // GIVEN
    val relElemType = RelationshipElementType(
      relTypeName("REL_TYPE"),
      NodeElementTypeReferenceByLabel(labelName("Label1")),
      NodeElementTypeReferenceByIdentifyingLabel(labelName("Label2")),
      Set.empty
    )

    // WHEN
    val result = GraphType.graphTypeInfoForPlan(Set(relElemType), Set.empty)

    // THEN
    result should equal(
      "{ (:Label1)-[:REL_TYPE =>]->(:Label2 =>) }"
    )
  }

  test("create single rel elem type with properties - forShowOutput=false") {
    // GIVEN
    val relElemType = RelationshipElementType(
      relTypeName("REL_TYPE"),
      EmptyNodeElementTypeReference,
      EmptyNodeElementTypeReference,
      Set(
        PropertyType(
          propKeyName("prop"),
          ClosedDynamicUnionType(Set(
            ListType(
              ClosedDynamicUnionType(Set(
                FloatType(isNullable = false)(pos),
                IntegerType(isNullable = false)(pos)
              ))(pos),
              isNullable = false
            )(pos),
            FloatType(isNullable = false)(pos),
            VectorType(Some(Integer16Type(isNullable = false)(pos)), Some(10), isNullable = false)(pos),
            IntegerType(isNullable = false)(pos)
          ))(pos)
        )
      )
    )

    // WHEN
    val result = GraphType.graphTypeInfoForPlan(Set(relElemType), Set.empty)

    // THEN
    result should equal(
      "{ ()-[:REL_TYPE => {prop :: INTEGER NOT NULL | FLOAT NOT NULL | VECTOR<INTEGER16 NOT NULL>(10) NOT NULL | LIST<INTEGER NOT NULL | FLOAT NOT NULL> NOT NULL}]->() }"
    )
  }

  test("create single rel elem type with full type - forShowOutput=false") {
    // GIVEN
    val relElemType = RelationshipElementType(
      relTypeName("REL_TYPE"),
      NodeElementTypeReferenceByIdentifyingLabel(labelName("Label1")),
      NodeElementTypeReferenceByLabel(labelName("Label2")),
      Set(
        PropertyType(propKeyName("prop1"), StringType(isNullable = true)(pos)),
        PropertyType(propKeyName("prop3"), StringType(isNullable = false)(pos)),
        PropertyType(propKeyName("prop2"), AnyType(isNullable = false)(pos))
      )
    )

    // WHEN
    val result = GraphType.graphTypeInfoForPlan(Set(relElemType), Set.empty)

    // THEN
    result should equal(
      "{ (:Label1 =>)-[:REL_TYPE => {prop1 :: STRING, prop2 :: ANY NOT NULL, prop3 :: STRING NOT NULL}]->(:Label2) }"
    )
  }

  test("create single independent constraint - forShowOutput=false") {
    Seq(None, Some("my_constraint")).foreach(name => {
      Seq(
        (NodeElementTypeReferenceByLabel(labelName("Label")), "(n:Label)", "n"),
        (RelationshipElementTypeReferenceByLabel(relTypeName("REL_TYPE")), "()-[r:REL_TYPE]->()", "r")
      ).foreach { case (reference, pattern, variable) =>
        Seq(
          (ExistenceConstraint, "IS NOT NULL"),
          (PropertyTypeConstraint(StringType(isNullable = true)(pos)), "IS :: STRING")
        ).foreach { case (constraintType, predicate) =>
          withClue(s"name: $name, pattern: $pattern, constraint: $predicate  -- ") {
            // GIVEN
            val constraint = GraphTypeCreateConstraint(
              name,
              reference,
              properties = ArraySeq(propKeyName("prop")),
              constraintType,
              options = NoOptions
            )

            // WHEN
            val result = GraphType.graphTypeInfoForPlan(Set.empty, Set(constraint))

            // THEN
            val nameString = name.map(n => s" $n").getOrElse("")
            result should equal(
              s"{ CONSTRAINT$nameString FOR $pattern REQUIRE $variable.prop $predicate }"
            )
          }
        }
      }
    })
  }

  test("create single undesignated constraint - forShowOutput=false") {
    Seq(None, Some("my_constraint")).foreach(name => {
      Seq(
        (NodeElementTypeReferenceByLabel(labelName("Label")), "(n:Label)", "n"),
        (RelationshipElementTypeReferenceByLabel(relTypeName("REL_TYPE")), "()-[r:REL_TYPE]->()", "r")
      ).foreach { case (reference, pattern, variable) =>
        Seq(
          (ArraySeq(propKeyName("prop")), s"$variable.prop"),
          (ArraySeq(propKeyName("prop2"), propKeyName("prop1")), s"($variable.prop2, $variable.prop1)")
        ).foreach { case (properties, propertiesString) =>
          Seq(
            (KeyConstraint, "IS KEY"),
            (UniquenessConstraint, "IS UNIQUE")
          ).foreach { case (constraintType, predicate) =>
            Seq(
              (OptionsMap(Map("indexConfig" -> MapExpression(Seq.empty)(pos)))(pos), " OPTIONS {indexConfig: {}}"),
              (OptionsParam(ExplicitParameter("param", CTMap)(pos))(pos), " OPTIONS $param"),
              (NoOptions, "")
            ).foreach { case (options, optionsString) =>
              withClue(
                s"name: $name, pattern: $pattern, properties: $propertiesString, constraint: $predicate, options:$optionsString -- "
              ) {
                // GIVEN
                val constraint = GraphTypeCreateConstraint(
                  name,
                  reference,
                  properties,
                  constraintType,
                  options
                )

                // WHEN
                val result = GraphType.graphTypeInfoForPlan(Set.empty, Set(constraint))

                // THEN
                val nameString = name.map(n => s" $n").getOrElse("")
                result should equal(
                  s"{ CONSTRAINT$nameString FOR $pattern REQUIRE $propertiesString $predicate$optionsString }"
                )
              }
            }
          }
        }
      }
    })
  }

  test("create full graph type - forShowOutput=false") {
    // GIVEN
    val nodeElemType1 = NodeElementType(
      labelName("Label1"),
      Set(labelName("Label7"), labelName("Label2")),
      Set(
        PropertyType(propKeyName("prop2"), StringType(isNullable = false)(pos)),
        PropertyType(
          propKeyName("prop1"),
          ClosedDynamicUnionType(Set(
            ListType(FloatType(isNullable = false)(pos), isNullable = true)(pos),
            VectorType(Some(FloatType(isNullable = false)(pos)), Some(42), isNullable = true)(pos)
          ))(pos)
        )
      )
    )
    val nodeElemType2 = NodeElementType(
      labelName("Label3"),
      Set.empty,
      Set(PropertyType(propKeyName("prop"), AnyType(isNullable = false)(pos)))
    )
    val relElemType1 = RelationshipElementType(
      relTypeName("REL_TYPE1"),
      NodeElementTypeReferenceByIdentifyingLabel(labelName("Label3")),
      NodeElementTypeReferenceByLabel(labelName("Label4")),
      Set(PropertyType(propKeyName("prop"), IntegerType(isNullable = false)(pos)))
    )
    val relElemType2 = RelationshipElementType(
      relTypeName("REL_TYPE2"),
      EmptyNodeElementTypeReference,
      NodeElementTypeReferenceByIdentifyingLabel(labelName("Label1")),
      Set(PropertyType(propKeyName("prop"), IntegerType(isNullable = true)(pos)))
    )
    val constraint1 = GraphTypeCreateConstraint(
      Some("c1"),
      NodeElementTypeReferenceByIdentifyingLabel(labelName("Label1")),
      ArraySeq(propKeyName("prop")),
      KeyConstraint,
      NoOptions
    )
    val constraint2 = GraphTypeCreateConstraint(
      Some("c2"),
      NodeElementTypeReferenceByLabel(labelName("Label4")),
      ArraySeq(propKeyName("prop")),
      ExistenceConstraint,
      NoOptions
    )
    val constraint3 = GraphTypeCreateConstraint(
      None,
      NodeElementTypeReferenceByLabel(labelName("Label5")),
      ArraySeq(propKeyName("prop")),
      ExistenceConstraint,
      NoOptions
    )
    val constraint4 = GraphTypeCreateConstraint(
      None,
      NodeElementTypeReferenceByLabel(labelName("Label5")),
      ArraySeq(propKeyName("prop2")),
      UniquenessConstraint,
      NoOptions
    )
    val constraint5 = GraphTypeCreateConstraint(
      Some("c3"),
      NodeElementTypeReferenceByLabel(labelName("Label6")),
      ArraySeq(propKeyName("prop")),
      PropertyTypeConstraint(DateType(isNullable = true)(pos)),
      NoOptions
    )
    val constraint6 = GraphTypeCreateConstraint(
      Some("c5"),
      RelationshipElementTypeReferenceByIdentifyingLabel(relTypeName("REL_TYPE1")),
      ArraySeq(propKeyName("prop")),
      UniquenessConstraint,
      NoOptions
    )
    val constraint7 = GraphTypeCreateConstraint(
      Some("c4"),
      RelationshipElementTypeReferenceByLabel(relTypeName("REL_TYPE3")),
      ArraySeq(propKeyName("prop")),
      KeyConstraint,
      NoOptions
    )
    val constraint8 = GraphTypeCreateConstraint(
      None,
      RelationshipElementTypeReferenceByLabel(relTypeName("REL_TYPE3")),
      ArraySeq(propKeyName("prop2")),
      KeyConstraint,
      NoOptions
    )
    val constraint9 = GraphTypeCreateConstraint(
      Some("c7"),
      RelationshipElementTypeReferenceByLabel(relTypeName("REL_TYPE4")),
      ArraySeq(propKeyName("prop")),
      ExistenceConstraint,
      NoOptions
    )
    val constraint10 = GraphTypeCreateConstraint(
      Some("c6"),
      RelationshipElementTypeReferenceByLabel(relTypeName("REL_TYPE4")),
      ArraySeq(propKeyName("prop")),
      PropertyTypeConstraint(DateType(isNullable = true)(pos)),
      NoOptions
    )

    // WHEN
    val result = GraphType.graphTypeInfoForPlan(
      Set(relElemType2, nodeElemType2, nodeElemType1, relElemType1),
      Set(
        constraint9,
        constraint5,
        constraint7,
        constraint4,
        constraint10,
        constraint2,
        constraint6,
        constraint1,
        constraint3,
        constraint8
      )
    )

    // THEN
    result should equal(
      "{ (:Label1 => :Label2&Label7 {prop1 :: VECTOR<FLOAT NOT NULL>(42) | LIST<FLOAT NOT NULL>, prop2 :: STRING NOT NULL}), (:Label3 => {prop :: ANY NOT NULL}), " +
        "(:Label3 =>)-[:REL_TYPE1 => {prop :: INTEGER NOT NULL}]->(:Label4), ()-[:REL_TYPE2 => {prop :: INTEGER}]->(:Label1 =>), " +
        "CONSTRAINT c1 FOR (n:Label1 =>) REQUIRE n.prop IS KEY, CONSTRAINT c2 FOR (n:Label4) REQUIRE n.prop IS NOT NULL, " +
        "CONSTRAINT FOR (n:Label5) REQUIRE n.prop IS NOT NULL, CONSTRAINT FOR (n:Label5) REQUIRE n.prop2 IS UNIQUE, CONSTRAINT c3 FOR (n:Label6) REQUIRE n.prop IS :: DATE, " +
        "CONSTRAINT c5 FOR ()-[r:REL_TYPE1 =>]->() REQUIRE r.prop IS UNIQUE, CONSTRAINT c4 FOR ()-[r:REL_TYPE3]->() REQUIRE r.prop IS KEY, " +
        "CONSTRAINT FOR ()-[r:REL_TYPE3]->() REQUIRE r.prop2 IS KEY, CONSTRAINT c7 FOR ()-[r:REL_TYPE4]->() REQUIRE r.prop IS NOT NULL, " +
        "CONSTRAINT c6 FOR ()-[r:REL_TYPE4]->() REQUIRE r.prop IS :: DATE }"
    )
  }

  test("create graph type that require escaping - forShowOutput=false") {
    // GIVEN
    val nodeElemType = NodeElementType(
      labelName("Label``2"),
      Set(labelName("Label-3"), labelName("Label!1")),
      Set(
        PropertyType(propKeyName("prop`2"), StringType(isNullable = true)(pos)),
        PropertyType(propKeyName("prop-1"), AnyType(isNullable = false)(pos))
      )
    )
    val relElemType = RelationshipElementType(
      relTypeName("REL-TYPE1"),
      NodeElementTypeReferenceByIdentifyingLabel(labelName("Label``2")),
      NodeElementTypeReferenceByLabel(labelName("Label-4")),
      Set(PropertyType(propKeyName("pr`op"), IntegerType(isNullable = false)(pos)))
    )
    val constraint1 = GraphTypeCreateConstraint(
      Some("c`2"),
      NodeElementTypeReferenceByIdentifyingLabel(labelName("Label``2")),
      ArraySeq(propKeyName("pr-op")),
      KeyConstraint,
      NoOptions
    )
    val constraint2 = GraphTypeCreateConstraint(
      Some("c-1"),
      NodeElementTypeReferenceByLabel(labelName("Label!1")),
      ArraySeq(propKeyName("pr`op")),
      ExistenceConstraint,
      NoOptions
    )
    val constraint3 = GraphTypeCreateConstraint(
      Some("c`4"),
      RelationshipElementTypeReferenceByLabel(relTypeName("REL-TYPE2")),
      ArraySeq(propKeyName("pr!op")),
      PropertyTypeConstraint(DateType(isNullable = true)(pos)),
      NoOptions
    )
    val constraint4 = GraphTypeCreateConstraint(
      Some("c!3"),
      RelationshipElementTypeReferenceByLabel(relTypeName("REL`TYPE3")),
      ArraySeq(propKeyName("pr%op")),
      UniquenessConstraint,
      NoOptions
    )

    // WHEN
    val result = GraphType.graphTypeInfoForPlan(
      Set(relElemType, nodeElemType),
      Set(constraint2, constraint4, constraint1, constraint3)
    )

    // THEN
    result should equal(
      "{ (:`Label````2` => :`Label!1`&`Label-3` {`prop-1` :: ANY NOT NULL, `prop``2` :: STRING}), " +
        "(:`Label````2` =>)-[:`REL-TYPE1` => {`pr``op` :: INTEGER NOT NULL}]->(:`Label-4`), " +
        "CONSTRAINT `c``2` FOR (n:`Label````2` =>) REQUIRE n.`pr-op` IS KEY, CONSTRAINT `c-1` FOR (n:`Label!1`) REQUIRE n.`pr``op` IS NOT NULL, " +
        "CONSTRAINT `c``4` FOR ()-[r:`REL-TYPE2`]->() REQUIRE r.`pr!op` IS :: DATE, CONSTRAINT `c!3` FOR ()-[r:`REL``TYPE3`]->() REQUIRE r.`pr%op` IS UNIQUE }"
    )
  }

  test("create single node elem type with implied label - forShowOutput=true") {
    // GIVEN
    val nodeElemType = NodeElementType(
      labelName("Label"),
      Set(labelName("Label2")),
      Set.empty
    )

    // WHEN
    val result = GraphType.graphTypeInfoForShow(Set(nodeElemType), Set.empty)

    // THEN
    result should equal(
      """{
        |  (:`Label` => :`Label2`)
        |}""".stripMargin
    )
  }

  test("create single node elem type with properties - forShowOutput=true") {
    // GIVEN
    val nodeElemType = NodeElementType(
      labelName("Label"),
      Set.empty,
      Set(
        PropertyType(propKeyName("prop3"), IntegerType(isNullable = false)(pos)),
        PropertyType(propKeyName("prop2"), IntegerType(isNullable = true)(pos)),
        PropertyType(propKeyName("prop1"), AnyType(isNullable = false)(pos))
      )
    )

    // WHEN
    val result = GraphType.graphTypeInfoForShow(Set(nodeElemType), Set.empty)

    // THEN
    result should equal(
      """{
        |  (:`Label` => {`prop1` :: ANY NOT NULL, `prop2` :: INTEGER, `prop3` :: INTEGER NOT NULL})
        |}""".stripMargin
    )
  }

  test("create single node elem type with full type - forShowOutput=true") {
    // GIVEN
    val nodeElemType = NodeElementType(
      labelName("Label"),
      Set(labelName("Label3"), labelName("Label2")),
      Set(PropertyType(propKeyName("prop"), IntegerType(isNullable = false)(pos)))
    )

    // WHEN
    val result = GraphType.graphTypeInfoForShow(Set(nodeElemType), Set.empty)

    // THEN
    result should equal(
      """{
        |  (:`Label` => :`Label2`&`Label3` {`prop` :: INTEGER NOT NULL})
        |}""".stripMargin
    )
  }

  test("create single rel elem type with source and target label - forShowOutput=true") {
    // GIVEN
    val relElemType = RelationshipElementType(
      relTypeName("REL_TYPE"),
      NodeElementTypeReferenceByLabel(labelName("Label1")),
      NodeElementTypeReferenceByIdentifyingLabel(labelName("Label2")),
      Set.empty
    )

    // WHEN
    val result = GraphType.graphTypeInfoForShow(Set(relElemType), Set.empty)

    // THEN
    result should equal(
      """{
        |  (:`Label1`)-[:`REL_TYPE` =>]->(:`Label2` =>)
        |}""".stripMargin
    )
  }

  test("create single rel elem type with properties - forShowOutput=true") {
    // GIVEN
    val relElemType = RelationshipElementType(
      relTypeName("REL_TYPE"),
      EmptyNodeElementTypeReference,
      EmptyNodeElementTypeReference,
      Set(
        PropertyType(
          propKeyName("prop"),
          ClosedDynamicUnionType(Set(
            ListType(
              ClosedDynamicUnionType(Set(
                FloatType(isNullable = false)(pos),
                IntegerType(isNullable = false)(pos)
              ))(pos),
              isNullable = false
            )(pos),
            FloatType(isNullable = false)(pos),
            VectorType(Some(Integer16Type(isNullable = false)(pos)), Some(10), isNullable = false)(pos),
            IntegerType(isNullable = false)(pos)
          ))(pos)
        )
      )
    )

    // WHEN
    val result = GraphType.graphTypeInfoForShow(Set(relElemType), Set.empty)

    // THEN
    result should equal(
      """{
        |  ()-[:`REL_TYPE` => {`prop` :: INTEGER NOT NULL | FLOAT NOT NULL | VECTOR<INTEGER16 NOT NULL>(10) NOT NULL | LIST<INTEGER NOT NULL | FLOAT NOT NULL> NOT NULL}]->()
        |}""".stripMargin
    )
  }

  test("create single rel elem type with full type - forShowOutput=true") {
    // GIVEN
    val relElemType = RelationshipElementType(
      relTypeName("REL_TYPE"),
      NodeElementTypeReferenceByIdentifyingLabel(labelName("Label1")),
      NodeElementTypeReferenceByLabel(labelName("Label2")),
      Set(
        PropertyType(propKeyName("prop1"), StringType(isNullable = true)(pos)),
        PropertyType(propKeyName("prop3"), StringType(isNullable = false)(pos)),
        PropertyType(propKeyName("prop2"), AnyType(isNullable = false)(pos))
      )
    )

    // WHEN
    val result = GraphType.graphTypeInfoForShow(Set(relElemType), Set.empty)

    // THEN
    result should equal(
      """{
        |  (:`Label1` =>)-[:`REL_TYPE` => {`prop1` :: STRING, `prop2` :: ANY NOT NULL, `prop3` :: STRING NOT NULL}]->(:`Label2`)
        |}""".stripMargin
    )
  }

  test("create single independent constraint - forShowOutput=true") {
    Seq(None, Some("my_constraint")).foreach(name => {
      Seq(
        (NodeElementTypeReferenceByLabel(labelName("Label")), "(`n`:`Label`)", "`n`"),
        (RelationshipElementTypeReferenceByLabel(relTypeName("REL_TYPE")), "()-[`r`:`REL_TYPE`]->()", "`r`")
      ).foreach { case (reference, pattern, variable) =>
        Seq(
          (ExistenceConstraint, "IS NOT NULL"),
          (PropertyTypeConstraint(StringType(isNullable = true)(pos)), "IS :: STRING")
        ).foreach { case (constraintType, predicate) =>
          withClue(s"name: $name, pattern: $pattern, constraint: $predicate -- ") {
            // GIVEN
            val constraint = GraphTypeCreateConstraint(
              name,
              reference,
              properties = ArraySeq(propKeyName("prop")),
              constraintType,
              options = NoOptions
            )

            // WHEN
            val result = GraphType.graphTypeInfoForShow(Set.empty, Set(constraint))

            // THEN
            val nameString = name.map(n => s" `$n`").getOrElse("")
            result should equal(
              s"""{
                 |  CONSTRAINT$nameString FOR $pattern REQUIRE ($variable.`prop`) $predicate
                 |}""".stripMargin
            )
          }
        }
      }
    })
  }

  test("create single undesignated constraint - forShowOutput=true") {
    Seq(None, Some("my_constraint")).foreach(name => {
      Seq(
        (NodeElementTypeReferenceByLabel(labelName("Label")), "(`n`:`Label`)", "`n`"),
        (RelationshipElementTypeReferenceByLabel(relTypeName("REL_TYPE")), "()-[`r`:`REL_TYPE`]->()", "`r`")
      ).foreach { case (reference, pattern, variable) =>
        Seq(
          (ArraySeq(propKeyName("prop")), s"($variable.`prop`)"),
          (ArraySeq(propKeyName("prop2"), propKeyName("prop1")), s"($variable.`prop2`, $variable.`prop1`)")
        ).foreach { case (properties, propertiesString) =>
          Seq(
            (KeyConstraint, "IS KEY"),
            (UniquenessConstraint, "IS UNIQUE")
          ).foreach { case (constraintType, predicate) =>
            Seq(
              (OptionsMap(Map("indexConfig" -> MapExpression(Seq.empty)(pos)))(pos), " OPTIONS {`indexConfig`: {}}"),
              (OptionsParam(ExplicitParameter("param", CTMap)(pos))(pos), " OPTIONS $`param`"),
              (NoOptions, "")
            ).foreach { case (options, optionsString) =>
              withClue(
                s"name: $name, pattern: $pattern, properties: $propertiesString, constraint: $predicate, options:$optionsString -- "
              ) {
                // GIVEN
                val constraint = GraphTypeCreateConstraint(
                  name,
                  reference,
                  properties,
                  constraintType,
                  options
                )

                // WHEN
                val result = GraphType.graphTypeInfoForShow(Set.empty, Set(constraint))

                // THEN
                val nameString = name.map(n => s" `$n`").getOrElse("")
                result should equal(
                  s"""{
                     |  CONSTRAINT$nameString FOR $pattern REQUIRE $propertiesString $predicate$optionsString
                     |}""".stripMargin
                )
              }
            }
          }
        }
      }
    })
  }

  test("create full graph type - forShowOutput=true") {
    // GIVEN (unnamed constraints doesn't make any sense for the show command but let's keep the same setup as the false case)
    val nodeElemType1 = NodeElementType(
      labelName("Label1"),
      Set(labelName("Label7"), labelName("Label2")),
      Set(
        PropertyType(propKeyName("prop2"), StringType(isNullable = false)(pos)),
        PropertyType(
          propKeyName("prop1"),
          ClosedDynamicUnionType(Set(
            ListType(FloatType(isNullable = false)(pos), isNullable = true)(pos),
            VectorType(Some(FloatType(isNullable = false)(pos)), Some(42), isNullable = true)(pos)
          ))(pos)
        )
      )
    )
    val nodeElemType2 = NodeElementType(
      labelName("Label3"),
      Set.empty,
      Set(PropertyType(propKeyName("prop"), AnyType(isNullable = false)(pos)))
    )
    val relElemType1 = RelationshipElementType(
      relTypeName("REL_TYPE1"),
      NodeElementTypeReferenceByIdentifyingLabel(labelName("Label3")),
      NodeElementTypeReferenceByLabel(labelName("Label4")),
      Set(PropertyType(propKeyName("prop"), IntegerType(isNullable = false)(pos)))
    )
    val relElemType2 = RelationshipElementType(
      relTypeName("REL_TYPE2"),
      EmptyNodeElementTypeReference,
      NodeElementTypeReferenceByIdentifyingLabel(labelName("Label1")),
      Set(PropertyType(propKeyName("prop"), IntegerType(isNullable = true)(pos)))
    )
    val constraint1 = GraphTypeCreateConstraint(
      Some("c1"),
      NodeElementTypeReferenceByIdentifyingLabel(labelName("Label1")),
      ArraySeq(propKeyName("prop")),
      KeyConstraint,
      NoOptions
    )
    val constraint2 = GraphTypeCreateConstraint(
      Some("c2"),
      NodeElementTypeReferenceByLabel(labelName("Label4")),
      ArraySeq(propKeyName("prop")),
      ExistenceConstraint,
      NoOptions
    )
    val constraint3 = GraphTypeCreateConstraint(
      None,
      NodeElementTypeReferenceByLabel(labelName("Label5")),
      ArraySeq(propKeyName("prop")),
      ExistenceConstraint,
      NoOptions
    )
    val constraint4 = GraphTypeCreateConstraint(
      None,
      NodeElementTypeReferenceByLabel(labelName("Label5")),
      ArraySeq(propKeyName("prop2")),
      UniquenessConstraint,
      NoOptions
    )
    val constraint5 = GraphTypeCreateConstraint(
      Some("c3"),
      NodeElementTypeReferenceByLabel(labelName("Label6")),
      ArraySeq(propKeyName("prop")),
      PropertyTypeConstraint(DateType(isNullable = true)(pos)),
      NoOptions
    )
    val constraint6 = GraphTypeCreateConstraint(
      Some("c5"),
      RelationshipElementTypeReferenceByIdentifyingLabel(relTypeName("REL_TYPE1")),
      ArraySeq(propKeyName("prop")),
      UniquenessConstraint,
      NoOptions
    )
    val constraint7 = GraphTypeCreateConstraint(
      Some("c4"),
      RelationshipElementTypeReferenceByLabel(relTypeName("REL_TYPE3")),
      ArraySeq(propKeyName("prop")),
      KeyConstraint,
      NoOptions
    )
    val constraint8 = GraphTypeCreateConstraint(
      None,
      RelationshipElementTypeReferenceByLabel(relTypeName("REL_TYPE3")),
      ArraySeq(propKeyName("prop2")),
      KeyConstraint,
      NoOptions
    )
    val constraint9 = GraphTypeCreateConstraint(
      Some("c7"),
      RelationshipElementTypeReferenceByLabel(relTypeName("REL_TYPE4")),
      ArraySeq(propKeyName("prop")),
      ExistenceConstraint,
      NoOptions
    )
    val constraint10 = GraphTypeCreateConstraint(
      Some("c6"),
      RelationshipElementTypeReferenceByLabel(relTypeName("REL_TYPE4")),
      ArraySeq(propKeyName("prop")),
      PropertyTypeConstraint(DateType(isNullable = true)(pos)),
      NoOptions
    )

    // WHEN
    val result = GraphType.graphTypeInfoForShow(
      Set(relElemType2, nodeElemType2, nodeElemType1, relElemType1),
      Set(
        constraint9,
        constraint5,
        constraint7,
        constraint4,
        constraint10,
        constraint2,
        constraint6,
        constraint1,
        constraint3,
        constraint8
      )
    )

    // THEN
    result should equal(
      """{
        |  (:`Label1` => :`Label2`&`Label7` {`prop1` :: VECTOR<FLOAT NOT NULL>(42) | LIST<FLOAT NOT NULL>, `prop2` :: STRING NOT NULL}),
        |  (:`Label3` => {`prop` :: ANY NOT NULL}),
        |  (:`Label3` =>)-[:`REL_TYPE1` => {`prop` :: INTEGER NOT NULL}]->(:`Label4`),
        |  ()-[:`REL_TYPE2` => {`prop` :: INTEGER}]->(:`Label1` =>),
        |  CONSTRAINT `c1` FOR (`n`:`Label1` =>) REQUIRE (`n`.`prop`) IS KEY,
        |  CONSTRAINT `c2` FOR (`n`:`Label4`) REQUIRE (`n`.`prop`) IS NOT NULL,
        |  CONSTRAINT FOR (`n`:`Label5`) REQUIRE (`n`.`prop`) IS NOT NULL,
        |  CONSTRAINT FOR (`n`:`Label5`) REQUIRE (`n`.`prop2`) IS UNIQUE,
        |  CONSTRAINT `c3` FOR (`n`:`Label6`) REQUIRE (`n`.`prop`) IS :: DATE,
        |  CONSTRAINT `c5` FOR ()-[`r`:`REL_TYPE1` =>]->() REQUIRE (`r`.`prop`) IS UNIQUE,
        |  CONSTRAINT `c4` FOR ()-[`r`:`REL_TYPE3`]->() REQUIRE (`r`.`prop`) IS KEY,
        |  CONSTRAINT FOR ()-[`r`:`REL_TYPE3`]->() REQUIRE (`r`.`prop2`) IS KEY,
        |  CONSTRAINT `c7` FOR ()-[`r`:`REL_TYPE4`]->() REQUIRE (`r`.`prop`) IS NOT NULL,
        |  CONSTRAINT `c6` FOR ()-[`r`:`REL_TYPE4`]->() REQUIRE (`r`.`prop`) IS :: DATE
        |}""".stripMargin
    )
  }

  test("create graph type that require escaping - forShowOutput=true") {
    // GIVEN
    val nodeElemType = NodeElementType(
      labelName("Label``2"),
      Set(labelName("Label-3"), labelName("Label!1")),
      Set(
        PropertyType(propKeyName("prop`2"), StringType(isNullable = true)(pos)),
        PropertyType(propKeyName("prop-1"), AnyType(isNullable = false)(pos))
      )
    )
    val relElemType = RelationshipElementType(
      relTypeName("REL-TYPE1"),
      NodeElementTypeReferenceByIdentifyingLabel(labelName("Label``2")),
      NodeElementTypeReferenceByLabel(labelName("Label-4")),
      Set(PropertyType(propKeyName("pr`op"), IntegerType(isNullable = false)(pos)))
    )
    val constraint1 = GraphTypeCreateConstraint(
      Some("c`2"),
      NodeElementTypeReferenceByIdentifyingLabel(labelName("Label``2")),
      ArraySeq(propKeyName("pr-op")),
      KeyConstraint,
      NoOptions
    )
    val constraint2 = GraphTypeCreateConstraint(
      Some("c-1"),
      NodeElementTypeReferenceByLabel(labelName("Label!1")),
      ArraySeq(propKeyName("pr`op")),
      ExistenceConstraint,
      NoOptions
    )
    val constraint3 = GraphTypeCreateConstraint(
      Some("c`4"),
      RelationshipElementTypeReferenceByLabel(relTypeName("REL-TYPE2")),
      ArraySeq(propKeyName("pr!op")),
      PropertyTypeConstraint(DateType(isNullable = true)(pos)),
      NoOptions
    )
    val constraint4 = GraphTypeCreateConstraint(
      Some("c!3"),
      RelationshipElementTypeReferenceByLabel(relTypeName("REL`TYPE3")),
      ArraySeq(propKeyName("pr%op")),
      UniquenessConstraint,
      NoOptions
    )

    // WHEN
    val result = GraphType.graphTypeInfoForShow(
      Set(relElemType, nodeElemType),
      Set(constraint2, constraint4, constraint1, constraint3)
    )

    // THEN
    result should equal(
      """{
        |  (:`Label````2` => :`Label!1`&`Label-3` {`prop-1` :: ANY NOT NULL, `prop``2` :: STRING}),
        |  (:`Label````2` =>)-[:`REL-TYPE1` => {`pr``op` :: INTEGER NOT NULL}]->(:`Label-4`),
        |  CONSTRAINT `c``2` FOR (`n`:`Label````2` =>) REQUIRE (`n`.`pr-op`) IS KEY,
        |  CONSTRAINT `c-1` FOR (`n`:`Label!1`) REQUIRE (`n`.`pr``op`) IS NOT NULL,
        |  CONSTRAINT `c``4` FOR ()-[`r`:`REL-TYPE2`]->() REQUIRE (`r`.`pr!op`) IS :: DATE,
        |  CONSTRAINT `c!3` FOR ()-[`r`:`REL``TYPE3`]->() REQUIRE (`r`.`pr%op`) IS UNIQUE
        |}""".stripMargin
    )
  }

  // Graph type info for DROP

  test("drop single node elem type by label") {
    // GIVEN
    val nodeElemType = NodeElementType(labelName("Label"), Set.empty, Set.empty)

    // WHEN
    val result = GraphType.graphTypeDropInfo(Set(nodeElemType), Set.empty)

    // THEN
    result should equal(
      "{ (:Label =>) }"
    )
  }

  test("drop single node elem type by full type") {
    // GIVEN
    val nodeElemType = NodeElementType(
      labelName("Label"),
      Set(labelName("Label2")),
      Set(PropertyType(propKeyName("prop"), IntegerType(isNullable = false)(pos)))
    )

    // WHEN
    val result = GraphType.graphTypeDropInfo(Set(nodeElemType), Set.empty)

    // THEN
    result should equal(
      "{ (:Label => :Label2 {prop :: INTEGER NOT NULL}) }"
    )
  }

  test("drop multiple node elem types") {
    // GIVEN
    val nodeElemType1 = NodeElementType(labelName("Label1"), Set.empty, Set.empty)
    val nodeElemType2 = NodeElementType(labelName("Label2"), Set.empty, Set.empty)

    // WHEN
    val result = GraphType.graphTypeDropInfo(Set(nodeElemType2, nodeElemType1), Set.empty)

    // THEN
    result should equal(
      "{ (:Label1 =>), (:Label2 =>) }"
    )
  }

  test("drop single rel elem type by relType") {
    // GIVEN
    val relElemType = RelationshipElementType(
      relTypeName("REL_TYPE"),
      EmptyNodeElementTypeReference,
      EmptyNodeElementTypeReference,
      Set.empty
    )

    // WHEN
    val result = GraphType.graphTypeDropInfo(Set(relElemType), Set.empty)

    // THEN
    result should equal(
      "{ ()-[:REL_TYPE =>]->() }"
    )
  }

  test("drop single rel elem type by full type") {
    // GIVEN
    val relElemType = RelationshipElementType(
      relTypeName("REL_TYPE"),
      NodeElementTypeReferenceByIdentifyingLabel(labelName("Label1")),
      NodeElementTypeReferenceByLabel(labelName("Label2")),
      Set(
        PropertyType(propKeyName("prop1"), StringType(isNullable = true)(pos)),
        PropertyType(propKeyName("prop2"), AnyType(isNullable = false)(pos))
      )
    )

    // WHEN
    val result = GraphType.graphTypeDropInfo(Set(relElemType), Set.empty)

    // THEN
    result should equal(
      "{ (:Label1 =>)-[:REL_TYPE => {prop1 :: STRING, prop2 :: ANY NOT NULL}]->(:Label2) }"
    )
  }

  test("drop multiple rel elem types") {
    // GIVEN
    val relElemType1 = RelationshipElementType(
      relTypeName("REL_TYPE1"),
      EmptyNodeElementTypeReference,
      EmptyNodeElementTypeReference,
      Set.empty
    )
    val relElemType2 = RelationshipElementType(
      relTypeName("REL_TYPE2"),
      EmptyNodeElementTypeReference,
      EmptyNodeElementTypeReference,
      Set.empty
    )

    // WHEN
    val result = GraphType.graphTypeDropInfo(Set(relElemType2, relElemType1), Set.empty)

    // THEN
    result should equal(
      "{ ()-[:REL_TYPE1 =>]->(), ()-[:REL_TYPE2 =>]->() }"
    )
  }

  test("drop single constraint") {
    // GIVEN
    val constraint = GraphTypeDropConstraint("my_constraint")

    // WHEN
    val result = GraphType.graphTypeDropInfo(Set.empty, Set(constraint))

    // THEN
    result should equal(
      "{ CONSTRAINT my_constraint }"
    )
  }

  test("drop multiple constraints") {
    // GIVEN
    val constraint1 = GraphTypeDropConstraint("my_first_constraint")
    val constraint2 = GraphTypeDropConstraint("my_second_constraint")

    // WHEN
    val result = GraphType.graphTypeDropInfo(Set.empty, Set(constraint2, constraint1))

    // THEN
    result should equal(
      "{ CONSTRAINT my_first_constraint, CONSTRAINT my_second_constraint }"
    )
  }

  test("drop full graph type") {
    // GIVEN
    val nodeElemType1 = NodeElementType(
      labelName("Label1"),
      Set(labelName("Label4"), labelName("Label2")),
      Set(PropertyType(propKeyName("prop"), IntegerType(isNullable = false)(pos)))
    )
    val nodeElemType2 = NodeElementType(labelName("Label3"), Set.empty, Set.empty)
    val nodeElemType3 = NodeElementType(
      labelName("Label5"),
      Set.empty,
      Set(PropertyType(
        propKeyName("prop"),
        ClosedDynamicUnionType(Set(
          ListType(
            ClosedDynamicUnionType(Set(FloatType(isNullable = false)(pos), IntegerType(isNullable = false)(pos)))(pos),
            isNullable = true
          )(pos),
          FloatType(isNullable = true)(pos),
          IntegerType(isNullable = true)(pos)
        ))(pos)
      ))
    )
    val relElemType1 = RelationshipElementType(
      relTypeName("REL_TYPE1"),
      EmptyNodeElementTypeReference,
      EmptyNodeElementTypeReference,
      Set.empty
    )
    val relElemType2 = RelationshipElementType(
      relTypeName("REL_TYPE2"),
      EmptyNodeElementTypeReference,
      NodeElementTypeReferenceByLabel(labelName("Label2")),
      Set(
        PropertyType(propKeyName("prop2"), AnyType(isNullable = false)(pos)),
        PropertyType(propKeyName("prop1"), StringType(isNullable = true)(pos))
      )
    )
    val relElemType3 = RelationshipElementType(
      relTypeName("REL_TYPE3"),
      NodeElementTypeReferenceByIdentifyingLabel(labelName("Label3")),
      EmptyNodeElementTypeReference,
      Set(
        PropertyType(propKeyName("prop2"), ListType(BooleanType(isNullable = false)(pos), isNullable = true)(pos)),
        PropertyType(propKeyName("prop1"), StringType(isNullable = false)(pos))
      )
    )
    val constraint1 = GraphTypeDropConstraint("my_first_constraint")
    val constraint2 = GraphTypeDropConstraint("my_second_constraint")

    // WHEN
    val result = GraphType.graphTypeDropInfo(
      Set(relElemType3, nodeElemType3, nodeElemType2, relElemType2, nodeElemType1, relElemType1),
      Set(constraint2, constraint1)
    )

    // THEN
    result should equal(
      "{ (:Label1 => :Label2&Label4 {prop :: INTEGER NOT NULL}), (:Label3 =>), (:Label5 => {prop :: INTEGER | FLOAT | LIST<INTEGER NOT NULL | FLOAT NOT NULL>}), " +
        "()-[:REL_TYPE1 =>]->(), ()-[:REL_TYPE2 => {prop1 :: STRING, prop2 :: ANY NOT NULL}]->(:Label2), (:Label3 =>)-[:REL_TYPE3 => {prop1 :: STRING NOT NULL, prop2 :: LIST<BOOLEAN NOT NULL>}]->(), " +
        "CONSTRAINT my_first_constraint, CONSTRAINT my_second_constraint }"
    )
  }

  test("drop graph type that require escaping") {
    // GIVEN
    val nodeElemType = NodeElementType(
      labelName("Label`1"),
      Set(labelName("Label-3"), labelName("Label!2")),
      Set(PropertyType(propKeyName("pr`op"), IntegerType(isNullable = false)(pos)))
    )
    val relElemType1 = RelationshipElementType(
      relTypeName("REL_TYPE-1"),
      EmptyNodeElementTypeReference,
      EmptyNodeElementTypeReference,
      Set.empty
    )
    val relElemType2 = RelationshipElementType(
      relTypeName("REL_TYPE`2"),
      NodeElementTypeReferenceByIdentifyingLabel(labelName("Label`1")),
      NodeElementTypeReferenceByLabel(labelName("Label%4")),
      Set(
        PropertyType(propKeyName("prop`1"), StringType(isNullable = true)(pos)),
        PropertyType(propKeyName("prop-2"), AnyType(isNullable = false)(pos))
      )
    )
    val constraint1 = GraphTypeDropConstraint("my-first-constraint")
    val constraint2 = GraphTypeDropConstraint("my`second`constraint")

    // WHEN
    val result = GraphType.graphTypeDropInfo(
      Set(relElemType2, relElemType1, nodeElemType),
      Set(constraint2, constraint1)
    )

    // THEN
    result should equal(
      "{ (:`Label``1` => :`Label!2`&`Label-3` {`pr``op` :: INTEGER NOT NULL}), " +
        "()-[:`REL_TYPE-1` =>]->(), (:`Label``1` =>)-[:`REL_TYPE``2` => {`prop-2` :: ANY NOT NULL, `prop``1` :: STRING}]->(:`Label%4`), " +
        "CONSTRAINT `my-first-constraint`, CONSTRAINT `my``second``constraint` }"
    )
  }

}
