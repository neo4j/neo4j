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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AllConstraints
import org.neo4j.cypher.internal.ast.AllDatabasesScope
import org.neo4j.cypher.internal.ast.AllExistsConstraints
import org.neo4j.cypher.internal.ast.AllFunctions
import org.neo4j.cypher.internal.ast.AllIndexes
import org.neo4j.cypher.internal.ast.BuiltInFunctions
import org.neo4j.cypher.internal.ast.CommaSeparatedNames
import org.neo4j.cypher.internal.ast.CurrentUser
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.ExpressionNames
import org.neo4j.cypher.internal.ast.FulltextIndexes
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.KeyConstraints
import org.neo4j.cypher.internal.ast.LookupIndexes
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.NoNames
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NodeAllExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKey
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.NodePropExistsConstraints
import org.neo4j.cypher.internal.ast.NodePropTypeConstraints
import org.neo4j.cypher.internal.ast.NodePropertyExistence
import org.neo4j.cypher.internal.ast.NodePropertyType
import org.neo4j.cypher.internal.ast.NodePropertyUniqueness
import org.neo4j.cypher.internal.ast.NodeUniqueConstraints
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.PointIndexes
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.PropExistsConstraints
import org.neo4j.cypher.internal.ast.PropTypeConstraints
import org.neo4j.cypher.internal.ast.RangeIndexes
import org.neo4j.cypher.internal.ast.RelAllExistsConstraints
import org.neo4j.cypher.internal.ast.RelKeyConstraints
import org.neo4j.cypher.internal.ast.RelPropExistsConstraints
import org.neo4j.cypher.internal.ast.RelPropTypeConstraints
import org.neo4j.cypher.internal.ast.RelUniqueConstraints
import org.neo4j.cypher.internal.ast.RelationshipKey
import org.neo4j.cypher.internal.ast.RelationshipPropertyExistence
import org.neo4j.cypher.internal.ast.RelationshipPropertyType
import org.neo4j.cypher.internal.ast.RelationshipPropertyUniqueness
import org.neo4j.cypher.internal.ast.SingleNamedDatabaseScope
import org.neo4j.cypher.internal.ast.TextIndexes
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.neo4j.cypher.internal.ast.User
import org.neo4j.cypher.internal.ast.UserDefinedFunctions
import org.neo4j.cypher.internal.ast.VectorIndexes
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.ResolvedNonLocalCall
import org.neo4j.cypher.internal.logical.plans.AlterCurrentGraphType
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.CommandDefaultColumn
import org.neo4j.cypher.internal.logical.plans.CommandYieldColumn
import org.neo4j.cypher.internal.logical.plans.CreateConstraint
import org.neo4j.cypher.internal.logical.plans.CreateFulltextIndex
import org.neo4j.cypher.internal.logical.plans.CreateIndex
import org.neo4j.cypher.internal.logical.plans.CreateLookupIndex
import org.neo4j.cypher.internal.logical.plans.CreateVectorIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForConstraint
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForFulltextIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForLookupIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForVectorIndex
import org.neo4j.cypher.internal.logical.plans.DropConstraintOnName
import org.neo4j.cypher.internal.logical.plans.DropIndexOnName
import org.neo4j.cypher.internal.logical.plans.EmptyNodeElementTypeReference
import org.neo4j.cypher.internal.logical.plans.ExistenceConstraint
import org.neo4j.cypher.internal.logical.plans.GraphTypeCreateConstraint
import org.neo4j.cypher.internal.logical.plans.GraphTypeDropConstraint
import org.neo4j.cypher.internal.logical.plans.GraphTypeForAdd
import org.neo4j.cypher.internal.logical.plans.GraphTypeForAlter
import org.neo4j.cypher.internal.logical.plans.GraphTypeForDrop
import org.neo4j.cypher.internal.logical.plans.GraphTypeForSet
import org.neo4j.cypher.internal.logical.plans.KeyConstraint
import org.neo4j.cypher.internal.logical.plans.NodeElementType
import org.neo4j.cypher.internal.logical.plans.NodeElementTypeReferenceByIdentifyingLabel
import org.neo4j.cypher.internal.logical.plans.NodeElementTypeReferenceByLabel
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.PropertyType
import org.neo4j.cypher.internal.logical.plans.PropertyTypeConstraint
import org.neo4j.cypher.internal.logical.plans.RelationshipElementType
import org.neo4j.cypher.internal.logical.plans.RelationshipElementTypeReferenceByIdentifyingLabel
import org.neo4j.cypher.internal.logical.plans.RelationshipElementTypeReferenceByLabel
import org.neo4j.cypher.internal.logical.plans.ShowConstraints
import org.neo4j.cypher.internal.logical.plans.ShowCurrentGraphType
import org.neo4j.cypher.internal.logical.plans.ShowDatabases
import org.neo4j.cypher.internal.logical.plans.ShowFunctions
import org.neo4j.cypher.internal.logical.plans.ShowIndexes
import org.neo4j.cypher.internal.logical.plans.ShowProcedures
import org.neo4j.cypher.internal.logical.plans.ShowSettings
import org.neo4j.cypher.internal.logical.plans.ShowTransactions
import org.neo4j.cypher.internal.logical.plans.TerminateTransactions
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UniquenessConstraint
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescriptionTestBase.details
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescriptionTestBase.planDescription
import org.neo4j.cypher.internal.util.symbols.AnyType
import org.neo4j.cypher.internal.util.symbols.BooleanType
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.DateType
import org.neo4j.cypher.internal.util.symbols.DurationType
import org.neo4j.cypher.internal.util.symbols.Float32Type
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.Integer32Type
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.LocalTimeType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.symbols.VectorType
import org.neo4j.cypher.internal.util.symbols.ZonedDateTimeType
import org.neo4j.graphdb.schema.IndexType

import scala.collection.immutable.ArraySeq

class SchemaAndNonAdminCommandsLogicalPlan2PlanDescriptionTest extends LogicalPlan2PlanDescriptionTestBase {

  // Index related commands

  test("CreateRangeIndex") {
    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.RANGE,
          label("Label"),
          List(key("prop")),
          Some(literalString("$indexName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("RANGE INDEX `$indexName` FOR (:Label) ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(CreateIndex(None, IndexType.RANGE, label("Label"), List(key("prop")), None, NoOptions), 1.0),
      planDescription(id, "CreateIndex", Seq.empty, Seq(details("RANGE INDEX FOR (:Label) ON (prop)")), Set.empty)
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.RANGE,
          label("Label"),
          List(key("prop")),
          Some(parameter("indexName", CTString)),
          OptionsMap(Map("indexProvider" -> stringLiteral("range-1.0")))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("""RANGE INDEX $indexName FOR (:Label) ON (prop) OPTIONS {indexProvider: "range-1.0"}""")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          Some(DoNothingIfExistsForIndex(label("Label"), List(key("prop")), IndexType.RANGE, None, NoOptions)),
          IndexType.RANGE,
          label("Label"),
          List(key("prop")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            Seq.empty,
            Seq(details("RANGE INDEX FOR (:Label) ON (prop)")),
            Set.empty
          )
        ),
        Seq(details("RANGE INDEX FOR (:Label) ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.RANGE,
          relType("Label"),
          List(key("prop")),
          Some(literalString("indexName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("RANGE INDEX indexName FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(CreateIndex(None, IndexType.RANGE, relType("Label"), List(key("prop")), None, NoOptions), 1.0),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("RANGE INDEX FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.RANGE,
          relType("Label"),
          List(key("prop")),
          Some(literalString("indexName")),
          OptionsMap(Map("indexProvider" -> stringLiteral("range-1.0")))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("""RANGE INDEX indexName FOR ()-[:Label]-() ON (prop) OPTIONS {indexProvider: "range-1.0"}""")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          Some(DoNothingIfExistsForIndex(relType("Label"), List(key("prop")), IndexType.RANGE, None, NoOptions)),
          IndexType.RANGE,
          relType("Label"),
          List(key("prop")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            Seq.empty,
            Seq(details("RANGE INDEX FOR ()-[:Label]-() ON (prop)")),
            Set.empty
          )
        ),
        Seq(details("RANGE INDEX FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.RANGE,
          label("Label"),
          List(key("prop1"), key("prop2")),
          Some(literalString("$indexName")),
          OptionsParam(parameter("options", CTMap))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("RANGE INDEX `$indexName` FOR (:Label) ON (prop1, prop2) OPTIONS $options")),
        Set.empty
      )
    )
  }

  test("CreateLookupIndex") {
    assertGood(
      attach(CreateLookupIndex(None, EntityType.NODE, Some(literalString("indexName")), NoOptions), 1.0),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("LOOKUP INDEX indexName FOR (n) ON EACH labels(n)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateLookupIndex(
          Some(DoNothingIfExistsForLookupIndex(EntityType.NODE, None, NoOptions)),
          EntityType.NODE,
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            Seq.empty,
            Seq(details("LOOKUP INDEX FOR (n) ON EACH labels(n)")),
            Set.empty
          )
        ),
        Seq(details("LOOKUP INDEX FOR (n) ON EACH labels(n)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateLookupIndex(
          None,
          EntityType.NODE,
          Some(literalString("$indexName")),
          OptionsMap(Map("indexProvider" -> stringLiteral("token-lookup-1.0")))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(
          details("""LOOKUP INDEX `$indexName` FOR (n) ON EACH labels(n) OPTIONS {indexProvider: "token-lookup-1.0"}""")
        ),
        Set.empty
      )
    )

    assertGood(
      attach(CreateLookupIndex(None, EntityType.RELATIONSHIP, Some(literalString("indexName")), NoOptions), 1.0),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("LOOKUP INDEX indexName FOR ()-[r]-() ON EACH type(r)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateLookupIndex(
          Some(DoNothingIfExistsForLookupIndex(
            EntityType.RELATIONSHIP,
            Some(parameter("indexName", CTString)),
            NoOptions
          )),
          EntityType.RELATIONSHIP,
          Some(parameter("indexName", CTString)),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            Seq.empty,
            Seq(details("LOOKUP INDEX $indexName FOR ()-[r]-() ON EACH type(r)")),
            Set.empty
          )
        ),
        Seq(details("LOOKUP INDEX $indexName FOR ()-[r]-() ON EACH type(r)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateLookupIndex(
          None,
          EntityType.RELATIONSHIP,
          Some(literalString("indexName")),
          OptionsMap(Map("indexConfig" -> MapExpression(Seq.empty)(pos)))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("LOOKUP INDEX indexName FOR ()-[r]-() ON EACH type(r) OPTIONS {indexConfig: {}}")),
        Set.empty
      )
    )
  }

  test("CreateFulltextIndex") {
    assertGood(
      attach(
        CreateFulltextIndex(
          None,
          Left(List(label("Label"))),
          List(key("prop")),
          Some(literalString("indexName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("FULLTEXT INDEX indexName FOR (:Label) ON EACH [prop]")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateFulltextIndex(None, Left(List(label("Label"))), List(key("prop1"), key("prop2")), None, NoOptions),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("FULLTEXT INDEX FOR (:Label) ON EACH [prop1, prop2]")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateFulltextIndex(
          None,
          Left(List(label("Label1"), label("Label2"))),
          List(key("prop")),
          Some(parameter("$indexName", CTString)),
          OptionsMap(Map("indexProvider" -> stringLiteral("fulltext-1.0")))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details(
          """FULLTEXT INDEX $`$indexName` FOR (:Label1|Label2) ON EACH [prop] OPTIONS {indexProvider: "fulltext-1.0"}"""
        )),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateFulltextIndex(
          Some(DoNothingIfExistsForFulltextIndex(Left(List(label("Label"))), List(key("prop")), None, NoOptions)),
          Left(List(label("Label"))),
          List(key("prop")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            Seq.empty,
            Seq(details("FULLTEXT INDEX FOR (:Label) ON EACH [prop]")),
            Set.empty
          )
        ),
        Seq(details("FULLTEXT INDEX FOR (:Label) ON EACH [prop]")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateFulltextIndex(
          None,
          Right(List(relType("Label"))),
          List(key("prop")),
          Some(literalString("indexName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("FULLTEXT INDEX indexName FOR ()-[:Label]-() ON EACH [prop]")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateFulltextIndex(
          None,
          Right(List(relType("Label"), relType("Type"))),
          List(key("prop1"), key("prop2")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("FULLTEXT INDEX FOR ()-[:Label|Type]-() ON EACH [prop1, prop2]")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateFulltextIndex(
          None,
          Right(List(relType("Label"))),
          List(key("prop")),
          Some(literalString("indexName")),
          OptionsMap(Map("indexProvider" -> stringLiteral("fulltext-1.0")))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details(
          """FULLTEXT INDEX indexName FOR ()-[:Label]-() ON EACH [prop] OPTIONS {indexProvider: "fulltext-1.0"}"""
        )),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateFulltextIndex(
          Some(DoNothingIfExistsForFulltextIndex(Right(List(relType("Label"))), List(key("prop")), None, NoOptions)),
          Right(List(relType("Label"))),
          List(key("prop")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            Seq.empty,
            Seq(details("FULLTEXT INDEX FOR ()-[:Label]-() ON EACH [prop]")),
            Set.empty
          )
        ),
        Seq(details("FULLTEXT INDEX FOR ()-[:Label]-() ON EACH [prop]")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateFulltextIndex(
          None,
          Left(List(label("Label"))),
          List(key("prop")),
          Some(literalString("indexName")),
          OptionsParam(parameter("ops", CTMap))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("FULLTEXT INDEX indexName FOR (:Label) ON EACH [prop] OPTIONS $ops")),
        Set.empty
      )
    )
  }

  test("CreateTextIndex") {
    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.TEXT,
          label("Label"),
          List(key("prop")),
          Some(literalString("indexName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("TEXT INDEX indexName FOR (:Label) ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(CreateIndex(None, IndexType.TEXT, label("Label"), List(key("prop")), None, NoOptions), 1.0),
      planDescription(id, "CreateIndex", Seq.empty, Seq(details("TEXT INDEX FOR (:Label) ON (prop)")), Set.empty)
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.TEXT,
          label("Label"),
          List(key("prop")),
          Some(parameter("indexName", CTString)),
          OptionsMap(Map("indexProvider" -> stringLiteral("text-1.0")))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("""TEXT INDEX $indexName FOR (:Label) ON (prop) OPTIONS {indexProvider: "text-1.0"}""")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          Some(DoNothingIfExistsForIndex(label("Label"), List(key("prop")), IndexType.TEXT, None, NoOptions)),
          IndexType.TEXT,
          label("Label"),
          List(key("prop")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            Seq.empty,
            Seq(details("TEXT INDEX FOR (:Label) ON (prop)")),
            Set.empty
          )
        ),
        Seq(details("TEXT INDEX FOR (:Label) ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.TEXT,
          relType("Label"),
          List(key("prop")),
          Some(literalString("indexName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("TEXT INDEX indexName FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(CreateIndex(None, IndexType.TEXT, relType("Label"), List(key("prop")), None, NoOptions), 1.0),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("TEXT INDEX FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.TEXT,
          relType("Label"),
          List(key("prop")),
          Some(literalString("indexName")),
          OptionsMap(Map("indexProvider" -> stringLiteral("text-1.0")))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("""TEXT INDEX indexName FOR ()-[:Label]-() ON (prop) OPTIONS {indexProvider: "text-1.0"}""")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          Some(DoNothingIfExistsForIndex(relType("Label"), List(key("prop")), IndexType.TEXT, None, NoOptions)),
          IndexType.TEXT,
          relType("Label"),
          List(key("prop")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            Seq.empty,
            Seq(details("TEXT INDEX FOR ()-[:Label]-() ON (prop)")),
            Set.empty
          )
        ),
        Seq(details("TEXT INDEX FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.TEXT,
          label("Label"),
          List(key("prop")),
          Some(literalString("indexName")),
          OptionsParam(parameter("options", CTMap))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("TEXT INDEX indexName FOR (:Label) ON (prop) OPTIONS $options")),
        Set.empty
      )
    )
  }

  test("CreatPointIndex") {
    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.POINT,
          label("Label"),
          List(key("prop")),
          Some(literalString("indexName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("POINT INDEX indexName FOR (:Label) ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(CreateIndex(None, IndexType.POINT, label("Label"), List(key("prop")), None, NoOptions), 1.0),
      planDescription(id, "CreateIndex", Seq.empty, Seq(details("POINT INDEX FOR (:Label) ON (prop)")), Set.empty)
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.POINT,
          label("Label"),
          List(key("prop")),
          Some(literalString("indexName")),
          OptionsMap(Map("indexProvider" -> stringLiteral("point-1.0")))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("""POINT INDEX indexName FOR (:Label) ON (prop) OPTIONS {indexProvider: "point-1.0"}""")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          Some(DoNothingIfExistsForIndex(
            label("Label"),
            List(key("prop")),
            IndexType.POINT,
            Some(parameter("indexName", CTString)),
            NoOptions
          )),
          IndexType.POINT,
          label("Label"),
          List(key("prop")),
          Some(parameter("indexName", CTString)),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            Seq.empty,
            Seq(details("POINT INDEX $indexName FOR (:Label) ON (prop)")),
            Set.empty
          )
        ),
        Seq(details("POINT INDEX $indexName FOR (:Label) ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.POINT,
          relType("Label"),
          List(key("prop")),
          Some(literalString("indexName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("POINT INDEX indexName FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(CreateIndex(None, IndexType.POINT, relType("Label"), List(key("prop")), None, NoOptions), 1.0),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("POINT INDEX FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.POINT,
          relType("Label"),
          List(key("prop")),
          Some(literalString("indexName")),
          OptionsMap(Map("indexProvider" -> stringLiteral("point-1.0")))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("""POINT INDEX indexName FOR ()-[:Label]-() ON (prop) OPTIONS {indexProvider: "point-1.0"}""")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          Some(DoNothingIfExistsForIndex(relType("Label"), List(key("prop")), IndexType.POINT, None, NoOptions)),
          IndexType.POINT,
          relType("Label"),
          List(key("prop")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            Seq.empty,
            Seq(details("POINT INDEX FOR ()-[:Label]-() ON (prop)")),
            Set.empty
          )
        ),
        Seq(details("POINT INDEX FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.POINT,
          label("Label"),
          List(key("prop")),
          Some(literalString("indexName")),
          OptionsParam(parameter("options", CTMap))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("POINT INDEX indexName FOR (:Label) ON (prop) OPTIONS $options")),
        Set.empty
      )
    )
  }

  test("CreateVectorIndex") {
    assertGood(
      attach(
        CreateVectorIndex(
          None,
          Left(List(label("Label"))),
          List(key("prop")),
          List.empty,
          Some(literalString("indexName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("VECTOR INDEX indexName FOR (:Label) ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(CreateVectorIndex(None, Left(List(label("Label"))), List(key("prop")), List.empty, None, NoOptions), 1.0),
      planDescription(id, "CreateIndex", Seq.empty, Seq(details("VECTOR INDEX FOR (:Label) ON (prop)")), Set.empty)
    )

    assertGood(
      attach(
        CreateVectorIndex(
          None,
          Left(List(label("Label1"), label("Label2"))),
          List(key("prop")),
          List.empty,
          Some(parameter("indexName", CTString)),
          OptionsMap(Map("indexProvider" -> stringLiteral("vector-1.0")))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(
          details("""VECTOR INDEX $indexName FOR (:Label1|Label2) ON (prop) OPTIONS {indexProvider: "vector-1.0"}""")
        ),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateVectorIndex(
          Some(DoNothingIfExistsForVectorIndex(
            Left(List(label("Label"))),
            List(key("prop")),
            List.empty,
            None,
            NoOptions
          )),
          Left(List(label("Label"))),
          List(key("prop")),
          List.empty,
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            Seq.empty,
            Seq(details("VECTOR INDEX FOR (:Label) ON (prop)")),
            Set.empty
          )
        ),
        Seq(details("VECTOR INDEX FOR (:Label) ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateVectorIndex(
          Some(DoNothingIfExistsForVectorIndex(
            Left(List(label("Label"))),
            List(key("prop")),
            List(key("prop2")),
            None,
            NoOptions
          )),
          Left(List(label("Label"))),
          List(key("prop")),
          List(key("prop2")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            Seq.empty,
            Seq(details("VECTOR INDEX FOR (:Label) ON (prop) WITH [prop2]")),
            Set.empty
          )
        ),
        Seq(details("VECTOR INDEX FOR (:Label) ON (prop) WITH [prop2]")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateVectorIndex(
          None,
          Right(List(relType("Label"))),
          List(key("prop")),
          List.empty,
          Some(literalString("indexName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("VECTOR INDEX indexName FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateVectorIndex(
          None,
          Right(List(relType("Label1"))),
          List(key("prop")),
          List(key("prop2"), key("prop3")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("VECTOR INDEX FOR ()-[:Label1]-() ON (prop) WITH [prop2, prop3]")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateVectorIndex(
          None,
          Right(List(relType("Label"))),
          List(key("prop")),
          List.empty,
          Some(literalString("indexName")),
          OptionsMap(Map("indexProvider" -> stringLiteral("vector-1.0")))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(
          details("""VECTOR INDEX indexName FOR ()-[:Label]-() ON (prop) OPTIONS {indexProvider: "vector-1.0"}""")
        ),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateVectorIndex(
          Some(DoNothingIfExistsForVectorIndex(
            Right(List(relType("Label"))),
            List(key("prop")),
            List.empty,
            None,
            NoOptions
          )),
          Right(List(relType("Label"))),
          List(key("prop")),
          List.empty,
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            Seq.empty,
            Seq(details("VECTOR INDEX FOR ()-[:Label]-() ON (prop)")),
            Set.empty
          )
        ),
        Seq(details("VECTOR INDEX FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateVectorIndex(
          Some(DoNothingIfExistsForVectorIndex(
            Right(List(relType("Label1"), relType("Label2"))),
            List(key("prop")),
            List.empty,
            None,
            NoOptions
          )),
          Right(List(relType("Label1"), relType("Label2"))),
          List(key("prop")),
          List.empty,
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            Seq.empty,
            Seq(details("VECTOR INDEX FOR ()-[:Label1|Label2]-() ON (prop)")),
            Set.empty
          )
        ),
        Seq(details("VECTOR INDEX FOR ()-[:Label1|Label2]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateVectorIndex(
          None,
          Left(List(label("Label"))),
          List(key("prop")),
          List.empty,
          Some(literalString("indexName")),
          OptionsParam(parameter("options", CTMap))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateIndex",
        Seq.empty,
        Seq(details("VECTOR INDEX indexName FOR (:Label) ON (prop) OPTIONS $options")),
        Set.empty
      )
    )
  }

  test("DropIndexOnName") {
    assertGood(
      attach(DropIndexOnName(literalString("indexName"), ifExists = false), 1.0),
      planDescription(id, "DropIndex", Seq.empty, Seq(details("INDEX indexName")), Set.empty)
    )

    assertGood(
      attach(DropIndexOnName(literalString("indexName"), ifExists = true), 1.0),
      planDescription(id, "DropIndex", Seq.empty, Seq(details("INDEX indexName IF EXISTS")), Set.empty)
    )

    assertGood(
      attach(DropIndexOnName(parameter("indexName", CTString), ifExists = false), 1.0),
      planDescription(id, "DropIndex", Seq.empty, Seq(details("INDEX $indexName")), Set.empty)
    )
  }

  test("ShowIndexes") {
    assertGood(
      attach(ShowIndexes(AllIndexes, List.empty, List.empty, yieldAll = true, Set.empty, Set.empty), 1.0),
      planDescription(id, "ShowIndexes", Seq.empty, Seq(details("allIndexes, allColumns")), Set.empty)
    )

    assertGood(
      attach(ShowIndexes(RangeIndexes, List.empty, List.empty, yieldAll = false, Set.empty, Set.empty), 1.0),
      planDescription(id, "ShowIndexes", Seq.empty, Seq(details("rangeIndexes, defaultColumns")), Set.empty)
    )

    assertGood(
      attach(ShowIndexes(FulltextIndexes, List.empty, List.empty, yieldAll = true, Set.empty, Set.empty), 1.0),
      planDescription(id, "ShowIndexes", Seq.empty, Seq(details("fulltextIndexes, allColumns")), Set.empty)
    )

    assertGood(
      attach(ShowIndexes(TextIndexes, List.empty, List.empty, yieldAll = true, Set.empty, Set.empty), 1.0),
      planDescription(id, "ShowIndexes", Seq.empty, Seq(details("textIndexes, allColumns")), Set.empty)
    )

    assertGood(
      attach(ShowIndexes(PointIndexes, List.empty, List.empty, yieldAll = true, Set.empty, Set.empty), 1.0),
      planDescription(id, "ShowIndexes", Seq.empty, Seq(details("pointIndexes, allColumns")), Set.empty)
    )

    assertGood(
      attach(ShowIndexes(VectorIndexes, List.empty, List.empty, yieldAll = true, Set.empty, Set.empty), 1.0),
      planDescription(id, "ShowIndexes", Seq.empty, Seq(details("vectorIndexes, allColumns")), Set.empty)
    )

    assertGood(
      attach(
        ShowIndexes(
          LookupIndexes,
          List(commandDefaultColumn("xxx"), commandDefaultColumn("yyy"), commandDefaultColumn("vvv")),
          List(
            CommandYieldColumn("xxx", "xxx"),
            CommandYieldColumn("yyy", "zzz"),
            CommandYieldColumn("vvv", "vvv")
          ),
          yieldAll = false,
          Set(varFor("xxx"), varFor("zzz"), varFor("vvv")),
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowIndexes",
        Seq.empty,
        Seq(details("lookupIndexes, columns(xxx, yyy AS zzz, vvv)")),
        Set("xxx", "zzz", "vvv")
      )
    )
  }

  // Constraint related commands

  test("CreateNodeUniquePropertyConstraint") {
    assertGood(
      attach(
        CreateConstraint(
          None,
          NodePropertyUniqueness.cypher5,
          label("Label"),
          Seq(prop(" x", "prop")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS UNIQUE")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          NodePropertyUniqueness.cypher25,
          label("Label"),
          Seq(prop("x", "prop")),
          Some(literalString("constraintName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT constraintName FOR (x:Label) REQUIRE (x.prop) IS UNIQUE")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          NodePropertyUniqueness.cypher5,
          label("Label"),
          Seq(prop("x", "prop1"), prop("x", "prop2")),
          Some(literalString("constraintName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT constraintName FOR (x:Label) REQUIRE (x.prop1, x.prop2) IS UNIQUE")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          NodePropertyUniqueness.cypher25,
          label("Label"),
          List(prop("x", "prop")),
          Some(literalString("$constraintName")),
          OptionsMap(Map("indexProvider" -> stringLiteral("range-1.0")))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details(
          """CONSTRAINT `$constraintName` FOR (x:Label) REQUIRE (x.prop) IS UNIQUE OPTIONS {indexProvider: "range-1.0"}"""
        )),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          Some(DoNothingIfExistsForConstraint(
            label("Label"),
            Seq(prop(" x", "prop")),
            NodePropertyUniqueness.cypher5,
            None,
            NoOptions
          )),
          NodePropertyUniqueness.cypher5,
          label("Label"),
          Seq(prop(" x", "prop")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(CONSTRAINT)",
            Seq.empty,
            Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS UNIQUE")),
            Set.empty
          )
        ),
        Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS UNIQUE")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          NodePropertyUniqueness.cypher25,
          label("Label"),
          Seq(prop(" x", "prop")),
          None,
          OptionsParam(parameter("options", CTMap))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS UNIQUE OPTIONS $options")),
        Set.empty
      )
    )
  }

  test("CreateRelationshipUniquePropertyConstraint") {
    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipPropertyUniqueness.cypher25,
          relType("REL_TYPE"),
          Seq(prop(" x", "prop")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT FOR ()-[` x`:REL_TYPE]-() REQUIRE (` x`.prop) IS UNIQUE")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipPropertyUniqueness.cypher5,
          relType("REL_TYPE"),
          Seq(prop("x", "prop")),
          Some(literalString("constraintName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT constraintName FOR ()-[x:REL_TYPE]-() REQUIRE (x.prop) IS UNIQUE")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipPropertyUniqueness.cypher25,
          relType("REL_TYPE"),
          Seq(prop("x", "prop1"), prop("x", "prop2")),
          Some(literalString("constraintName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT constraintName FOR ()-[x:REL_TYPE]-() REQUIRE (x.prop1, x.prop2) IS UNIQUE")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipPropertyUniqueness.cypher5,
          relType("REL-TYPE"),
          List(prop("x", "prop-prop")),
          Some(parameter("constraintName", CTString)),
          OptionsMap(Map("indexProvider" -> stringLiteral("range-1.0")))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details(
          """CONSTRAINT $constraintName FOR ()-[x:`REL-TYPE`]-() REQUIRE (x.`prop-prop`) IS UNIQUE OPTIONS {indexProvider: "range-1.0"}"""
        )),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          Some(DoNothingIfExistsForConstraint(
            relType("REL_TYPE"),
            Seq(prop(" x", "prop")),
            RelationshipPropertyUniqueness.cypher25,
            None,
            NoOptions
          )),
          RelationshipPropertyUniqueness.cypher25,
          relType("REL_TYPE"),
          Seq(prop(" x", "prop")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(CONSTRAINT)",
            Seq.empty,
            Seq(details("CONSTRAINT FOR ()-[` x`:REL_TYPE]-() REQUIRE (` x`.prop) IS UNIQUE")),
            Set.empty
          )
        ),
        Seq(details("CONSTRAINT FOR ()-[` x`:REL_TYPE]-() REQUIRE (` x`.prop) IS UNIQUE")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipPropertyUniqueness.cypher25,
          relType("REL_TYPE"),
          Seq(prop(" x", "prop")),
          None,
          OptionsParam(parameter("options", CTMap))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT FOR ()-[` x`:REL_TYPE]-() REQUIRE (` x`.prop) IS UNIQUE OPTIONS $options")),
        Set.empty
      )
    )
  }

  test("CreateNodeKeyConstraint") {
    assertGood(
      attach(CreateConstraint(None, NodeKey.cypher5, label("Label"), Seq(prop(" x", "prop")), None, NoOptions), 1.0),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS NODE KEY")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          NodeKey.cypher5,
          label("Label"),
          Seq(prop("x", "prop1"), prop("x", "prop2")),
          Some(literalString("constraintName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT constraintName FOR (x:Label) REQUIRE (x.prop1, x.prop2) IS NODE KEY")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          NodeKey.cypher5,
          label("Label"),
          List(prop("x", "prop")),
          Some(parameter("constraintName", CTString)),
          OptionsMap(Map("indexProvider" -> stringLiteral("range-1.0")))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details(
          """CONSTRAINT $constraintName FOR (x:Label) REQUIRE (x.prop) IS NODE KEY OPTIONS {indexProvider: "range-1.0"}"""
        )),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          Some(DoNothingIfExistsForConstraint(
            label("Label"),
            Seq(prop(" x", "prop")),
            NodeKey.cypher5,
            Some(literalString("constraintName")),
            NoOptions
          )),
          NodeKey.cypher5,
          label("Label"),
          Seq(prop(" x", "prop")),
          Some(literalString("constraintName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(CONSTRAINT)",
            Seq.empty,
            Seq(details("CONSTRAINT constraintName FOR (` x`:Label) REQUIRE (` x`.prop) IS NODE KEY")),
            Set.empty
          )
        ),
        Seq(details("CONSTRAINT constraintName FOR (` x`:Label) REQUIRE (` x`.prop) IS NODE KEY")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          NodeKey.cypher5,
          label("Label"),
          Seq(prop(" x", "prop")),
          None,
          OptionsParam(parameter("options", CTMap))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS NODE KEY OPTIONS $options")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          NodeKey.cypher25,
          label("Label"),
          Seq(prop("x", "prop1"), prop("x", "prop2")),
          Some(literalString("constraintName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT constraintName FOR (x:Label) REQUIRE (x.prop1, x.prop2) IS KEY")),
        Set.empty
      ),
      cypherVersion = CypherVersion.Cypher25
    )
  }

  test("CreateRelationshipKeyConstraint") {
    assertGood(
      attach(
        CreateConstraint(None, RelationshipKey.cypher5, relType("REL_TYPE"), Seq(prop(" x", "prop")), None, NoOptions),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT FOR ()-[` x`:REL_TYPE]-() REQUIRE (` x`.prop) IS RELATIONSHIP KEY")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipKey.cypher5,
          relType("REL_TYPE"),
          Seq(prop("x", "prop1"), prop("x", "prop2")),
          Some(literalString("constraintName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT constraintName FOR ()-[x:REL_TYPE]-() REQUIRE (x.prop1, x.prop2) IS RELATIONSHIP KEY")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipKey.cypher5,
          relType("REL_TYPE"),
          List(prop("x", "prop")),
          Some(parameter("constraintName", CTString)),
          OptionsMap(Map("indexProvider" -> stringLiteral("range-1.0")))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details(
          """CONSTRAINT $constraintName FOR ()-[x:REL_TYPE]-() REQUIRE (x.prop) IS RELATIONSHIP KEY OPTIONS {indexProvider: "range-1.0"}"""
        )),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          Some(DoNothingIfExistsForConstraint(
            relType("REL_TYPE"),
            Seq(prop(" x", "prop")),
            RelationshipKey.cypher5,
            Some(literalString("constraintName")),
            NoOptions
          )),
          RelationshipKey.cypher5,
          relType("REL_TYPE"),
          Seq(prop(" x", "prop")),
          Some(literalString("constraintName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(CONSTRAINT)",
            Seq.empty,
            Seq(details("CONSTRAINT constraintName FOR ()-[` x`:REL_TYPE]-() REQUIRE (` x`.prop) IS RELATIONSHIP KEY")),
            Set.empty
          )
        ),
        Seq(details("CONSTRAINT constraintName FOR ()-[` x`:REL_TYPE]-() REQUIRE (` x`.prop) IS RELATIONSHIP KEY")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipKey.cypher5,
          relType("REL_TYPE"),
          Seq(prop(" x", "prop")),
          None,
          OptionsParam(parameter("options", CTMap))(pos)
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT FOR ()-[` x`:REL_TYPE]-() REQUIRE (` x`.prop) IS RELATIONSHIP KEY OPTIONS $options")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(None, RelationshipKey.cypher25, relType("REL_TYPE"), Seq(prop(" x", "prop")), None, NoOptions),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT FOR ()-[` x`:REL_TYPE]-() REQUIRE (` x`.prop) IS KEY")),
        Set.empty
      ),
      cypherVersion = CypherVersion.Cypher25
    )
  }

  test("CreateNodePropertyExistenceConstraint") {
    assertGood(
      attach(
        CreateConstraint(None, NodePropertyExistence, label("Label"), Seq(prop(" x", "prop")), None, NoOptions),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS NOT NULL")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          NodePropertyExistence,
          label("Label"),
          Seq(prop("x", "prop")),
          Some(literalString("constraintName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT constraintName FOR (x:Label) REQUIRE (x.prop) IS NOT NULL")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          Some(DoNothingIfExistsForConstraint(
            label("Label"),
            Seq(prop(" x", "prop")),
            NodePropertyExistence,
            None,
            NoOptions
          )),
          NodePropertyExistence,
          label("Label"),
          Seq(prop(" x", "prop")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(CONSTRAINT)",
            Seq.empty,
            Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS NOT NULL")),
            Set.empty
          )
        ),
        Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS NOT NULL")),
        Set.empty
      )
    )
  }

  test("CreateRelationshipPropertyExistenceConstraint") {
    assertGood(
      attach(
        CreateConstraint(None, RelationshipPropertyExistence, relType("R"), Seq(prop(" x", "prop")), None, NoOptions),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT FOR ()-[` x`:R]-() REQUIRE (` x`.prop) IS NOT NULL")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipPropertyExistence,
          relType("R"),
          Seq(prop(" x", "prop")),
          Some(literalString("constraintName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT constraintName FOR ()-[` x`:R]-() REQUIRE (` x`.prop) IS NOT NULL")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          Some(DoNothingIfExistsForConstraint(
            relType("R"),
            Seq(prop(" x", "prop")),
            RelationshipPropertyExistence,
            None,
            NoOptions
          )),
          RelationshipPropertyExistence,
          relType("R"),
          Seq(prop(" x", "prop")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(CONSTRAINT)",
            Seq.empty,
            Seq(details("CONSTRAINT FOR ()-[` x`:R]-() REQUIRE (` x`.prop) IS NOT NULL")),
            Set.empty
          )
        ),
        Seq(details("CONSTRAINT FOR ()-[` x`:R]-() REQUIRE (` x`.prop) IS NOT NULL")),
        Set.empty
      )
    )
  }

  test("CreateNodePropertyTypeConstraint") {
    assertGood(
      attach(
        CreateConstraint(
          None,
          NodePropertyType(IntegerType(isNullable = true)(pos)),
          label("Label"),
          Seq(prop(" x", "prop")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS :: INTEGER")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          NodePropertyType(
            ClosedDynamicUnionType(Set(
              ListType(BooleanType(isNullable = true)(pos), isNullable = true)(pos),
              StringType(isNullable = true)(pos)
            ))(pos)
          ),
          label("Label"),
          Seq(prop("x", "prop")),
          Some(literalString("constraintName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT constraintName FOR (x:Label) REQUIRE (x.prop) IS :: STRING | LIST<BOOLEAN>")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          Some(DoNothingIfExistsForConstraint(
            label("Label"),
            Seq(prop(" x", "prop")),
            NodePropertyType(ZonedDateTimeType(isNullable = true)(pos)),
            None,
            NoOptions
          )),
          NodePropertyType(ZonedDateTimeType(isNullable = true)(pos)),
          label("Label"),
          Seq(prop(" x", "prop")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(CONSTRAINT)",
            Seq.empty,
            Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS :: ZONED DATETIME")),
            Set.empty
          )
        ),
        Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS :: ZONED DATETIME")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          NodePropertyType(VectorType(Some(Integer32Type(isNullable = false)(pos)), Some(3), isNullable = true)(pos)),
          label("Label"),
          Seq(prop("x", "prop")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT FOR (x:Label) REQUIRE (x.prop) IS :: VECTOR<INTEGER32 NOT NULL>(3)")),
        Set.empty
      )
    )
  }

  test("CreateRelationshipPropertyTypeConstraint") {
    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipPropertyType(FloatType(isNullable = true)(pos)),
          relType("R"),
          Seq(prop(" x", "prop")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT FOR ()-[` x`:R]-() REQUIRE (` x`.prop) IS :: FLOAT")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipPropertyType(LocalTimeType(isNullable = true)(pos)),
          relType("R"),
          Seq(prop(" x", "prop")),
          Some(literalString("constraintName")),
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details("CONSTRAINT constraintName FOR ()-[` x`:R]-() REQUIRE (` x`.prop) IS :: LOCAL TIME")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          Some(DoNothingIfExistsForConstraint(
            relType("R"),
            Seq(prop(" x", "prop")),
            RelationshipPropertyType(
              ClosedDynamicUnionType(Set(
                DurationType(isNullable = true)(pos),
                DateType(isNullable = true)(pos)
              ))(pos)
            ),
            None,
            NoOptions
          )),
          RelationshipPropertyType(
            ClosedDynamicUnionType(Set(
              DurationType(isNullable = true)(pos),
              DateType(isNullable = true)(pos)
            ))(pos)
          ),
          relType("R"),
          Seq(prop(" x", "prop")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq(
          planDescription(
            id,
            "DoNothingIfExists(CONSTRAINT)",
            Seq.empty,
            Seq(details("CONSTRAINT FOR ()-[` x`:R]-() REQUIRE (` x`.prop) IS :: DATE | DURATION")),
            Set.empty
          )
        ),
        Seq(details("CONSTRAINT FOR ()-[` x`:R]-() REQUIRE (` x`.prop) IS :: DATE | DURATION")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipPropertyType(
            ClosedDynamicUnionType(Set(
              ListType(FloatType(isNullable = false)(pos), isNullable = true)(pos),
              VectorType(Some(FloatType(isNullable = false)(pos)), Some(128), isNullable = true)(pos)
            ))(pos)
          ),
          relType("R"),
          Seq(prop("x", "prop")),
          None,
          NoOptions
        ),
        1.0
      ),
      planDescription(
        id,
        "CreateConstraint",
        Seq.empty,
        Seq(details(
          "CONSTRAINT FOR ()-[x:R]-() REQUIRE (x.prop) IS :: VECTOR<FLOAT NOT NULL>(128) | LIST<FLOAT NOT NULL>"
        )),
        Set.empty
      )
    )
  }

  test("DropConstraintOnName") {
    assertGood(
      attach(DropConstraintOnName(literalString("name"), ifExists = false), 1.0),
      planDescription(id, "DropConstraint", Seq.empty, Seq(details("CONSTRAINT name")), Set.empty)
    )

    assertGood(
      attach(DropConstraintOnName(literalString("name"), ifExists = true), 1.0),
      planDescription(id, "DropConstraint", Seq.empty, Seq(details("CONSTRAINT name IF EXISTS")), Set.empty)
    )

    assertGood(
      attach(DropConstraintOnName(parameter("name", CTString), ifExists = false), 1.0),
      planDescription(id, "DropConstraint", Seq.empty, Seq(details("CONSTRAINT $name")), Set.empty)
    )
  }

  test("ShowConstraints") {
    assertGood(
      attach(
        ShowConstraints(
          constraintType = AllConstraints,
          List.empty,
          List.empty,
          yieldAll = false,
          Set.empty,
          Set.empty
        ),
        1.0
      ),
      planDescription(id, "ShowConstraints", Seq.empty, Seq(details("allConstraints, defaultColumns")), Set.empty)
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = UniqueConstraints.cypher25,
          List.empty,
          List.empty,
          yieldAll = true,
          Set.empty,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowConstraints",
        Seq.empty,
        Seq(details("propertyUniquenessConstraints, allColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = NodeUniqueConstraints.cypher5,
          List.empty,
          List.empty,
          yieldAll = true,
          Set.empty,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowConstraints",
        Seq.empty,
        Seq(details("nodeUniquenessConstraints, allColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = RelUniqueConstraints.cypher25,
          List.empty,
          List.empty,
          yieldAll = true,
          Set.empty,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowConstraints",
        Seq.empty,
        Seq(details("relationshipPropertyUniquenessConstraints, allColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = KeyConstraints,
          List.empty,
          List.empty,
          yieldAll = false,
          Set.empty,
          Set.empty
        ),
        1.0
      ),
      planDescription(id, "ShowConstraints", Seq.empty, Seq(details("keyConstraints, defaultColumns")), Set.empty)
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = NodeKeyConstraints,
          List.empty,
          List.empty,
          yieldAll = false,
          Set.empty,
          Set.empty
        ),
        1.0
      ),
      planDescription(id, "ShowConstraints", Seq.empty, Seq(details("nodeKeyConstraints, defaultColumns")), Set.empty)
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = RelKeyConstraints,
          List.empty,
          List.empty,
          yieldAll = false,
          Set.empty,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowConstraints",
        Seq.empty,
        Seq(details("relationshipKeyConstraints, defaultColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = PropExistsConstraints.cypher5,
          List.empty,
          List.empty,
          yieldAll = true,
          Set.empty,
          Set.empty
        ),
        1.0
      ),
      planDescription(id, "ShowConstraints", Seq.empty, Seq(details("existenceConstraints, allColumns")), Set.empty)
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = NodePropExistsConstraints.cypher25,
          List.empty,
          List.empty,
          yieldAll = false,
          Set.empty,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowConstraints",
        Seq.empty,
        Seq(details("nodePropertyExistenceConstraints, defaultColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = RelPropExistsConstraints.cypher5,
          List.empty,
          List.empty,
          yieldAll = true,
          Set.empty,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowConstraints",
        Seq.empty,
        Seq(details("relationshipExistenceConstraints, allColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = AllExistsConstraints,
          List.empty,
          List.empty,
          yieldAll = true,
          Set.empty,
          Set.empty
        ),
        1.0
      ),
      planDescription(id, "ShowConstraints", Seq.empty, Seq(details("existenceConstraints, allColumns")), Set.empty)
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = NodeAllExistsConstraints,
          List.empty,
          List.empty,
          yieldAll = false,
          Set.empty,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowConstraints",
        Seq.empty,
        Seq(details("nodeExistenceConstraints, defaultColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = RelAllExistsConstraints,
          List.empty,
          List.empty,
          yieldAll = true,
          Set.empty,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowConstraints",
        Seq.empty,
        Seq(details("relationshipExistenceConstraints, allColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = PropTypeConstraints,
          List.empty,
          List.empty,
          yieldAll = false,
          Set.empty,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowConstraints",
        Seq.empty,
        Seq(details("propertyTypeConstraints, defaultColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = NodePropTypeConstraints,
          List.empty,
          List.empty,
          yieldAll = true,
          Set.empty,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowConstraints",
        Seq.empty,
        Seq(details("nodePropertyTypeConstraints, allColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = RelPropTypeConstraints,
          List(commandDefaultColumn("xxx"), commandDefaultColumn("yyy"), commandDefaultColumn("vvv")),
          List(
            CommandYieldColumn("xxx", "xxx"),
            CommandYieldColumn("yyy", "zzz"),
            CommandYieldColumn("vvv", "vvv")
          ),
          yieldAll = false,
          Set(varFor("xxx"), varFor("zzz"), varFor("vvv")),
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowConstraints",
        Seq.empty,
        Seq(details("relationshipPropertyTypeConstraints, columns(xxx, yyy AS zzz, vvv)")),
        Set("xxx", "zzz", "vvv")
      )
    )
  }

  // Graph type commands

  test("AlterCurrentGraphType - set, empty graph type") {
    assertGood(
      attach(
        AlterCurrentGraphType(GraphTypeForSet(Set.empty, Set.empty)),
        1.0
      ),
      planDescription(id, "AlterCurrentGraphTypeSet", Seq.empty, Seq(details("{}")), Set.empty)
    )
  }

  test("AlterCurrentGraphType - set, node and rel elem types") {
    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForSet(
            Set(
              NodeElementType(
                label("L"),
                Set.empty,
                Set(PropertyType(propName("p"), IntegerType(isNullable = true)(pos)))
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(id, "AlterCurrentGraphTypeSet", Seq.empty, Seq(details("{ (:L => {p :: INTEGER}) }")), Set.empty)
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForSet(
            Set(
              NodeElementType(
                label("L"),
                Set.empty,
                Set(PropertyType(
                  propName("p"),
                  VectorType(Some(IntegerType(isNullable = false)(pos)), Some(1234), isNullable = true)(pos)
                ))
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeSet",
        Seq.empty,
        Seq(details("{ (:L => {p :: VECTOR<INTEGER NOT NULL>(1234)}) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForSet(
            Set(
              NodeElementType(
                label("L1"),
                Set(label("L2")),
                Set.empty
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(id, "AlterCurrentGraphTypeSet", Seq.empty, Seq(details("{ (:L1 => :L2) }")), Set.empty)
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForSet(
            Set(
              NodeElementType(
                label("L1"),
                Set(label("L3"), label("L2")),
                Set(
                  PropertyType(propName("p1"), IntegerType(isNullable = false)(pos)),
                  PropertyType(propName("p2"), BooleanType(isNullable = true)(pos))
                )
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeSet",
        Seq.empty,
        Seq(details("{ (:L1 => :L2&L3 {p1 :: INTEGER NOT NULL, p2 :: BOOLEAN}) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForSet(
            Set(
              RelationshipElementType(
                relType("R"),
                EmptyNodeElementTypeReference,
                EmptyNodeElementTypeReference,
                Set(PropertyType(propName("p"), IntegerType(isNullable = true)(pos)))
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeSet",
        Seq.empty,
        Seq(details("{ ()-[:R => {p :: INTEGER}]->() }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForSet(
            Set(
              RelationshipElementType(
                relType("R"),
                NodeElementTypeReferenceByIdentifyingLabel(label("L2")),
                NodeElementTypeReferenceByLabel(label("L1")),
                Set.empty
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeSet",
        Seq.empty,
        Seq(details("{ (:L2 =>)-[:R =>]->(:L1) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForSet(
            Set(
              RelationshipElementType(
                relType("R"),
                NodeElementTypeReferenceByIdentifyingLabel(label("L")),
                EmptyNodeElementTypeReference,
                Set.empty
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeSet",
        Seq.empty,
        Seq(details("{ (:L =>)-[:R =>]->() }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForSet(
            Set(
              RelationshipElementType(
                relType("R"),
                EmptyNodeElementTypeReference,
                NodeElementTypeReferenceByLabel(label("L")),
                Set.empty
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeSet",
        Seq.empty,
        Seq(details("{ ()-[:R =>]->(:L) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForSet(
            Set(
              RelationshipElementType(
                relType("R"),
                EmptyNodeElementTypeReference,
                NodeElementTypeReferenceByIdentifyingLabel(label("L")),
                Set(
                  PropertyType(propName("p2"), DateType(isNullable = false)(pos)),
                  PropertyType(propName("p1"), StringType(isNullable = false)(pos))
                )
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeSet",
        Seq.empty,
        Seq(details("{ ()-[:R => {p1 :: STRING NOT NULL, p2 :: DATE NOT NULL}]->(:L =>) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForSet(
            Set(
              RelationshipElementType(
                relType("R"),
                NodeElementTypeReferenceByLabel(label("L1")),
                NodeElementTypeReferenceByIdentifyingLabel(label("L2")),
                Set(PropertyType(propName("p"), IntegerType(isNullable = true)(pos)))
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeSet",
        Seq.empty,
        Seq(details("{ (:L1)-[:R => {p :: INTEGER}]->(:L2 =>) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForSet(
            Set(
              RelationshipElementType(
                relType("R2"),
                NodeElementTypeReferenceByLabel(label("L2")),
                NodeElementTypeReferenceByIdentifyingLabel(label("L1")),
                Set(
                  PropertyType(propName("p2"), DateType(isNullable = true)(pos)),
                  PropertyType(propName("p3"), FloatType(isNullable = true)(pos)),
                  PropertyType(propName("p1"), IntegerType(isNullable = true)(pos))
                )
              ),
              NodeElementType(
                label("L1"),
                Set(label("L3"), label("L4"), label("L2")),
                Set(
                  PropertyType(propName("p2"), BooleanType(isNullable = true)(pos)),
                  PropertyType(propName("p1"), StringType(isNullable = false)(pos))
                )
              ),
              NodeElementType(
                label("L5"),
                Set.empty,
                Set(
                  PropertyType(propName("p2"), AnyType(isNullable = false)(pos)),
                  PropertyType(propName("p1"), IntegerType(isNullable = true)(pos))
                )
              ),
              RelationshipElementType(
                relType("R1"),
                EmptyNodeElementTypeReference,
                NodeElementTypeReferenceByLabel(label("L5")),
                Set.empty
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeSet",
        Seq.empty,
        Seq(details(
          "{ (:L1 => :L2&L3&L4 {p1 :: STRING NOT NULL, p2 :: BOOLEAN}), (:L5 => {p1 :: INTEGER, p2 :: ANY NOT NULL}), " +
            "()-[:R1 =>]->(:L5), (:L2)-[:R2 => {p1 :: INTEGER, p2 :: DATE, p3 :: FLOAT}]->(:L1 =>) }"
        )),
        Set.empty
      )
    )
  }

  test("AlterCurrentGraphType - set, constraints") {
    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForSet(
            Set.empty,
            Set(
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByLabel(label("L")),
                ArraySeq(propName("p")),
                ExistenceConstraint,
                NoOptions
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeSet",
        Seq.empty,
        Seq(details("{ CONSTRAINT FOR (n:L) REQUIRE n.p IS NOT NULL }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForSet(
            Set.empty,
            Set(
              GraphTypeCreateConstraint(
                None,
                RelationshipElementTypeReferenceByLabel(relType("R")),
                ArraySeq(propName("p")),
                ExistenceConstraint,
                NoOptions
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeSet",
        Seq.empty,
        Seq(details("{ CONSTRAINT FOR ()-[r:R]->() REQUIRE r.p IS NOT NULL }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForSet(
            Set.empty,
            Set(
              GraphTypeCreateConstraint(
                Some("c"),
                NodeElementTypeReferenceByLabel(label("L")),
                ArraySeq(propName("p")),
                PropertyTypeConstraint(IntegerType(isNullable = true)(pos)),
                NoOptions
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeSet",
        Seq.empty,
        Seq(details("{ CONSTRAINT c FOR (n:L) REQUIRE n.p IS :: INTEGER }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForSet(
            Set.empty,
            Set(
              GraphTypeCreateConstraint(
                None,
                RelationshipElementTypeReferenceByLabel(relType("R")),
                ArraySeq(propName("p")),
                PropertyTypeConstraint(StringType(isNullable = true)(pos)),
                NoOptions
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeSet",
        Seq.empty,
        Seq(details("{ CONSTRAINT FOR ()-[r:R]->() REQUIRE r.p IS :: STRING }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForSet(
            Set.empty,
            Set(
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByLabel(label("L")),
                ArraySeq(propName("p")),
                UniquenessConstraint,
                OptionsMap(Map(
                  "index\nConfig" -> mapOf("invalid\nToHave\nConfig" -> stringLiteral("butFails\nInRuntime"))
                ))(pos)
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeSet",
        Seq.empty,
        Seq(details(
          "{ CONSTRAINT FOR (n:L) REQUIRE n.p IS UNIQUE OPTIONS {`index Config`: {`invalid ToHave Config`: \"butFails InRuntime\"}} }"
        )),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForSet(
            Set.empty,
            Set(
              GraphTypeCreateConstraint(
                Some("c"),
                RelationshipElementTypeReferenceByIdentifyingLabel(relType("R")),
                ArraySeq(propName("p")),
                UniquenessConstraint,
                NoOptions
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeSet",
        Seq.empty,
        Seq(details("{ CONSTRAINT c FOR ()-[r:R =>]->() REQUIRE r.p IS UNIQUE }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForSet(
            Set.empty,
            Set(
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByIdentifyingLabel(label("L")),
                ArraySeq(propName("p")),
                KeyConstraint,
                NoOptions
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeSet",
        Seq.empty,
        Seq(details("{ CONSTRAINT FOR (n:L =>) REQUIRE n.p IS KEY }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForSet(
            Set.empty,
            Set(
              GraphTypeCreateConstraint(
                None,
                RelationshipElementTypeReferenceByLabel(relType("R")),
                ArraySeq(propName("p")),
                KeyConstraint,
                NoOptions
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeSet",
        Seq.empty,
        Seq(details("{ CONSTRAINT FOR ()-[r:R]->() REQUIRE r.p IS KEY }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForSet(
            Set.empty,
            Set(
              GraphTypeCreateConstraint(
                Some("c2"),
                RelationshipElementTypeReferenceByLabel(relType("R1")),
                ArraySeq(propName("p2"), propName("p1")),
                KeyConstraint,
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByLabel(label("L3")),
                ArraySeq(propName("p2")),
                PropertyTypeConstraint(IntegerType(isNullable = true)(pos)),
                NoOptions
              ),
              GraphTypeCreateConstraint(
                Some("c1"),
                NodeElementTypeReferenceByLabel(label("L3")),
                ArraySeq(propName("p1")),
                ExistenceConstraint,
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                RelationshipElementTypeReferenceByIdentifyingLabel(relType("R2")),
                ArraySeq(propName("p")),
                UniquenessConstraint,
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                RelationshipElementTypeReferenceByLabel(relType("R3")),
                ArraySeq(propName("p1")),
                PropertyTypeConstraint(StringType(isNullable = true)(pos)),
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByLabel(label("L2")),
                ArraySeq(propName("p2"), propName("p3"), propName("p1")),
                UniquenessConstraint,
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByIdentifyingLabel(label("L1")),
                ArraySeq(propName("p")),
                KeyConstraint,
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                RelationshipElementTypeReferenceByLabel(relType("R3")),
                ArraySeq(propName("p1")),
                ExistenceConstraint,
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByIdentifyingLabel(label("L4")),
                ArraySeq(propName("p1"), propName("p2"), propName("p3")),
                KeyConstraint,
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByIdentifyingLabel(label("L4")),
                ArraySeq(propName("p2"), propName("p1")),
                KeyConstraint,
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByIdentifyingLabel(label("L4")),
                ArraySeq(propName("p1"), propName("p2")),
                UniquenessConstraint,
                NoOptions
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeSet",
        Seq.empty,
        Seq(details(
          "{ CONSTRAINT FOR (n:L1 =>) REQUIRE n.p IS KEY, " +
            "CONSTRAINT FOR (n:L4 =>) REQUIRE (n.p1, n.p2) IS UNIQUE, " +
            "CONSTRAINT FOR (n:L4 =>) REQUIRE (n.p1, n.p2, n.p3) IS KEY, " +
            "CONSTRAINT FOR (n:L4 =>) REQUIRE (n.p2, n.p1) IS KEY, " +
            "CONSTRAINT FOR (n:L2) REQUIRE (n.p2, n.p3, n.p1) IS UNIQUE, " +
            "CONSTRAINT c1 FOR (n:L3) REQUIRE n.p1 IS NOT NULL, " +
            "CONSTRAINT FOR (n:L3) REQUIRE n.p2 IS :: INTEGER, " +
            "CONSTRAINT FOR ()-[r:R2 =>]->() REQUIRE r.p IS UNIQUE, " +
            "CONSTRAINT c2 FOR ()-[r:R1]->() REQUIRE (r.p2, r.p1) IS KEY, " +
            "CONSTRAINT FOR ()-[r:R3]->() REQUIRE r.p1 IS NOT NULL, " +
            "CONSTRAINT FOR ()-[r:R3]->() REQUIRE r.p1 IS :: STRING }"
        )),
        Set.empty
      )
    )
  }

  test("AlterCurrentGraphType - set, mixed") {
    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForSet(
            Set(
              RelationshipElementType(
                relType("R-\n2"),
                NodeElementTypeReferenceByLabel(label("L!\n2")),
                NodeElementTypeReferenceByIdentifyingLabel(label("L`1")),
                Set(
                  PropertyType(propName("1p2"), DateType(isNullable = true)(pos)),
                  PropertyType(propName("p-3"), FloatType(isNullable = true)(pos)),
                  PropertyType(propName("p%1"), IntegerType(isNullable = true)(pos))
                )
              ),
              NodeElementType(
                label("L-1"),
                Set(label("L-3"), label("L-4"), label("L-2")),
                Set(
                  PropertyType(propName("p`2"), BooleanType(isNullable = true)(pos)),
                  PropertyType(propName("p`\n1"), StringType(isNullable = false)(pos))
                )
              ),
              NodeElementType(
                label("L-5"),
                Set.empty,
                Set(
                  PropertyType(propName("p-2"), AnyType(isNullable = false)(pos)),
                  PropertyType(propName("p-1"), IntegerType(isNullable = true)(pos))
                )
              ),
              RelationshipElementType(
                relType("R%1"),
                EmptyNodeElementTypeReference,
                NodeElementTypeReferenceByLabel(label("L%5")),
                Set.empty
              )
            ),
            Set(
              GraphTypeCreateConstraint(
                Some("c\n-2"),
                RelationshipElementTypeReferenceByLabel(relType("R-1")),
                ArraySeq(propName("p-2"), propName("p-1")),
                KeyConstraint,
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByLabel(label("L%3")),
                ArraySeq(propName("p`\n2")),
                PropertyTypeConstraint(IntegerType(isNullable = true)(pos)),
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByIdentifyingLabel(label("L!2")),
                ArraySeq(propName("2p2"), propName("3p3"), propName("1p1")),
                UniquenessConstraint,
                NoOptions
              ),
              GraphTypeCreateConstraint(
                Some("c-3"),
                RelationshipElementTypeReferenceByLabel(relType("R`\n3")),
                ArraySeq(propName("p-1")),
                ExistenceConstraint,
                NoOptions
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeSet",
        Seq.empty,
        Seq(details(
          "{ (:`L-1` => :`L-2`&`L-3`&`L-4` {`p`` 1` :: STRING NOT NULL, `p``2` :: BOOLEAN}), " +
            "(:`L-5` => {`p-1` :: INTEGER, `p-2` :: ANY NOT NULL}), " +
            "()-[:`R%1` =>]->(:`L%5`), " +
            "(:`L! 2`)-[:`R- 2` => {`1p2` :: DATE, `p%1` :: INTEGER, `p-3` :: FLOAT}]->(:`L``1` =>), " +
            "CONSTRAINT FOR (n:`L!2` =>) REQUIRE (n.`2p2`, n.`3p3`, n.`1p1`) IS UNIQUE, " +
            "CONSTRAINT FOR (n:`L%3`) REQUIRE n.`p`` 2` IS :: INTEGER, " +
            "CONSTRAINT `c -2` FOR ()-[r:`R-1`]->() REQUIRE (r.`p-2`, r.`p-1`) IS KEY, " +
            "CONSTRAINT `c-3` FOR ()-[r:`R`` 3`]->() REQUIRE r.`p-1` IS NOT NULL }"
        )),
        Set.empty
      )
    )
  }

  test("AlterCurrentGraphType - add, empty graph type") {
    assertGood(
      attach(
        AlterCurrentGraphType(GraphTypeForAdd(Set.empty, Set.empty)),
        1.0
      ),
      planDescription(id, "AlterCurrentGraphTypeAdd", Seq.empty, Seq(details("{}")), Set.empty)
    )
  }

  test("AlterCurrentGraphType - add, node and rel elem types") {
    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAdd(
            Set(
              NodeElementType(
                label("L"),
                Set.empty,
                Set(PropertyType(propName("p"), IntegerType(isNullable = true)(pos)))
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(id, "AlterCurrentGraphTypeAdd", Seq.empty, Seq(details("{ (:L => {p :: INTEGER}) }")), Set.empty)
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAdd(
            Set(
              NodeElementType(
                label("L1"),
                Set(label("L2")),
                Set.empty
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(id, "AlterCurrentGraphTypeAdd", Seq.empty, Seq(details("{ (:L1 => :L2) }")), Set.empty)
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAdd(
            Set(
              NodeElementType(
                label("L1"),
                Set(label("L3"), label("L2")),
                Set(
                  PropertyType(propName("p1"), IntegerType(isNullable = false)(pos)),
                  PropertyType(propName("p2"), BooleanType(isNullable = true)(pos))
                )
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAdd",
        Seq.empty,
        Seq(details("{ (:L1 => :L2&L3 {p1 :: INTEGER NOT NULL, p2 :: BOOLEAN}) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAdd(
            Set(
              RelationshipElementType(
                relType("R"),
                EmptyNodeElementTypeReference,
                EmptyNodeElementTypeReference,
                Set(PropertyType(propName("p"), IntegerType(isNullable = true)(pos)))
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAdd",
        Seq.empty,
        Seq(details("{ ()-[:R => {p :: INTEGER}]->() }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAdd(
            Set(
              RelationshipElementType(
                relType("R"),
                NodeElementTypeReferenceByIdentifyingLabel(label("L2")),
                NodeElementTypeReferenceByLabel(label("L1")),
                Set.empty
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAdd",
        Seq.empty,
        Seq(details("{ (:L2 =>)-[:R =>]->(:L1) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAdd(
            Set(
              RelationshipElementType(
                relType("R"),
                NodeElementTypeReferenceByIdentifyingLabel(label("L")),
                EmptyNodeElementTypeReference,
                Set.empty
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAdd",
        Seq.empty,
        Seq(details("{ (:L =>)-[:R =>]->() }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAdd(
            Set(
              RelationshipElementType(
                relType("R"),
                EmptyNodeElementTypeReference,
                NodeElementTypeReferenceByLabel(label("L")),
                Set.empty
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAdd",
        Seq.empty,
        Seq(details("{ ()-[:R =>]->(:L) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAdd(
            Set(
              RelationshipElementType(
                relType("R"),
                EmptyNodeElementTypeReference,
                NodeElementTypeReferenceByIdentifyingLabel(label("L")),
                Set(
                  PropertyType(propName("p2"), DateType(isNullable = false)(pos)),
                  PropertyType(propName("p1"), StringType(isNullable = false)(pos))
                )
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAdd",
        Seq.empty,
        Seq(details("{ ()-[:R => {p1 :: STRING NOT NULL, p2 :: DATE NOT NULL}]->(:L =>) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAdd(
            Set(
              RelationshipElementType(
                relType("R"),
                NodeElementTypeReferenceByLabel(label("L1")),
                NodeElementTypeReferenceByIdentifyingLabel(label("L2")),
                Set(PropertyType(propName("p"), IntegerType(isNullable = true)(pos)))
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAdd",
        Seq.empty,
        Seq(details("{ (:L1)-[:R => {p :: INTEGER}]->(:L2 =>) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAdd(
            Set(
              RelationshipElementType(
                relType("R2"),
                NodeElementTypeReferenceByLabel(label("L2")),
                NodeElementTypeReferenceByIdentifyingLabel(label("L1")),
                Set(
                  PropertyType(propName("p2"), DateType(isNullable = true)(pos)),
                  PropertyType(propName("p3"), FloatType(isNullable = true)(pos)),
                  PropertyType(propName("p1"), IntegerType(isNullable = true)(pos))
                )
              ),
              NodeElementType(
                label("L1"),
                Set(label("L3"), label("L4"), label("L2")),
                Set(
                  PropertyType(propName("p2"), BooleanType(isNullable = true)(pos)),
                  PropertyType(propName("p1"), StringType(isNullable = false)(pos))
                )
              ),
              NodeElementType(
                label("L5"),
                Set.empty,
                Set(
                  PropertyType(propName("p2"), AnyType(isNullable = false)(pos)),
                  PropertyType(propName("p1"), IntegerType(isNullable = true)(pos))
                )
              ),
              RelationshipElementType(
                relType("R1"),
                EmptyNodeElementTypeReference,
                NodeElementTypeReferenceByLabel(label("L5")),
                Set.empty
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAdd",
        Seq.empty,
        Seq(details(
          "{ (:L1 => :L2&L3&L4 {p1 :: STRING NOT NULL, p2 :: BOOLEAN}), (:L5 => {p1 :: INTEGER, p2 :: ANY NOT NULL}), " +
            "()-[:R1 =>]->(:L5), (:L2)-[:R2 => {p1 :: INTEGER, p2 :: DATE, p3 :: FLOAT}]->(:L1 =>) }"
        )),
        Set.empty
      )
    )
  }

  test("AlterCurrentGraphType - add, constraints") {
    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAdd(
            Set.empty,
            Set(
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByLabel(label("L")),
                ArraySeq(propName("p")),
                ExistenceConstraint,
                NoOptions
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAdd",
        Seq.empty,
        Seq(details("{ CONSTRAINT FOR (n:L) REQUIRE n.p IS NOT NULL }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAdd(
            Set.empty,
            Set(
              GraphTypeCreateConstraint(
                None,
                RelationshipElementTypeReferenceByLabel(relType("R")),
                ArraySeq(propName("p")),
                ExistenceConstraint,
                NoOptions
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAdd",
        Seq.empty,
        Seq(details("{ CONSTRAINT FOR ()-[r:R]->() REQUIRE r.p IS NOT NULL }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAdd(
            Set.empty,
            Set(
              GraphTypeCreateConstraint(
                Some("c"),
                NodeElementTypeReferenceByLabel(label("L")),
                ArraySeq(propName("p")),
                PropertyTypeConstraint(IntegerType(isNullable = true)(pos)),
                NoOptions
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAdd",
        Seq.empty,
        Seq(details("{ CONSTRAINT c FOR (n:L) REQUIRE n.p IS :: INTEGER }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAdd(
            Set.empty,
            Set(
              GraphTypeCreateConstraint(
                None,
                RelationshipElementTypeReferenceByLabel(relType("R")),
                ArraySeq(propName("p")),
                PropertyTypeConstraint(StringType(isNullable = true)(pos)),
                NoOptions
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAdd",
        Seq.empty,
        Seq(details("{ CONSTRAINT FOR ()-[r:R]->() REQUIRE r.p IS :: STRING }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAdd(
            Set.empty,
            Set(
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByLabel(label("L")),
                ArraySeq(propName("p")),
                UniquenessConstraint,
                NoOptions
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAdd",
        Seq.empty,
        Seq(details("{ CONSTRAINT FOR (n:L) REQUIRE n.p IS UNIQUE }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAdd(
            Set.empty,
            Set(
              GraphTypeCreateConstraint(
                Some("c"),
                RelationshipElementTypeReferenceByIdentifyingLabel(relType("R")),
                ArraySeq(propName("p")),
                UniquenessConstraint,
                NoOptions
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAdd",
        Seq.empty,
        Seq(details("{ CONSTRAINT c FOR ()-[r:R =>]->() REQUIRE r.p IS UNIQUE }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAdd(
            Set.empty,
            Set(
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByIdentifyingLabel(label("L")),
                ArraySeq(propName("p")),
                KeyConstraint,
                NoOptions
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAdd",
        Seq.empty,
        Seq(details("{ CONSTRAINT FOR (n:L =>) REQUIRE n.p IS KEY }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAdd(
            Set.empty,
            Set(
              GraphTypeCreateConstraint(
                None,
                RelationshipElementTypeReferenceByLabel(relType("R")),
                ArraySeq(propName("p")),
                KeyConstraint,
                OptionsMap(Map.empty)(pos)
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAdd",
        Seq.empty,
        Seq(details("{ CONSTRAINT FOR ()-[r:R]->() REQUIRE r.p IS KEY OPTIONS {} }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAdd(
            Set.empty,
            Set(
              GraphTypeCreateConstraint(
                Some("c2"),
                RelationshipElementTypeReferenceByLabel(relType("R1")),
                ArraySeq(propName("p2"), propName("p1")),
                KeyConstraint,
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByLabel(label("L3")),
                ArraySeq(propName("p2")),
                PropertyTypeConstraint(IntegerType(isNullable = true)(pos)),
                NoOptions
              ),
              GraphTypeCreateConstraint(
                Some("c1"),
                NodeElementTypeReferenceByLabel(label("L3")),
                ArraySeq(propName("p1")),
                ExistenceConstraint,
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                RelationshipElementTypeReferenceByIdentifyingLabel(relType("R2")),
                ArraySeq(propName("p")),
                UniquenessConstraint,
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                RelationshipElementTypeReferenceByLabel(relType("R3")),
                ArraySeq(propName("p1")),
                PropertyTypeConstraint(StringType(isNullable = true)(pos)),
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByLabel(label("L2")),
                ArraySeq(propName("p2"), propName("p3"), propName("p1")),
                UniquenessConstraint,
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByIdentifyingLabel(label("L1")),
                ArraySeq(propName("p")),
                KeyConstraint,
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                RelationshipElementTypeReferenceByLabel(relType("R3")),
                ArraySeq(propName("p1")),
                ExistenceConstraint,
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByIdentifyingLabel(label("L4")),
                ArraySeq(propName("p1"), propName("p2"), propName("p3")),
                KeyConstraint,
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByIdentifyingLabel(label("L4")),
                ArraySeq(propName("p2"), propName("p1")),
                KeyConstraint,
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByIdentifyingLabel(label("L4")),
                ArraySeq(propName("p1"), propName("p2")),
                UniquenessConstraint,
                NoOptions
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAdd",
        Seq.empty,
        Seq(details(
          "{ CONSTRAINT FOR (n:L1 =>) REQUIRE n.p IS KEY, " +
            "CONSTRAINT FOR (n:L4 =>) REQUIRE (n.p1, n.p2) IS UNIQUE, " +
            "CONSTRAINT FOR (n:L4 =>) REQUIRE (n.p1, n.p2, n.p3) IS KEY, " +
            "CONSTRAINT FOR (n:L4 =>) REQUIRE (n.p2, n.p1) IS KEY, " +
            "CONSTRAINT FOR (n:L2) REQUIRE (n.p2, n.p3, n.p1) IS UNIQUE, " +
            "CONSTRAINT c1 FOR (n:L3) REQUIRE n.p1 IS NOT NULL, " +
            "CONSTRAINT FOR (n:L3) REQUIRE n.p2 IS :: INTEGER, " +
            "CONSTRAINT FOR ()-[r:R2 =>]->() REQUIRE r.p IS UNIQUE, " +
            "CONSTRAINT c2 FOR ()-[r:R1]->() REQUIRE (r.p2, r.p1) IS KEY, " +
            "CONSTRAINT FOR ()-[r:R3]->() REQUIRE r.p1 IS NOT NULL, " +
            "CONSTRAINT FOR ()-[r:R3]->() REQUIRE r.p1 IS :: STRING }"
        )),
        Set.empty
      )
    )
  }

  test("AlterCurrentGraphType - add, mixed") {
    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAdd(
            Set(
              RelationshipElementType(
                relType("R-2"),
                NodeElementTypeReferenceByLabel(label("L!2")),
                NodeElementTypeReferenceByIdentifyingLabel(label("L`1")),
                Set(
                  PropertyType(propName("1p2"), DateType(isNullable = true)(pos)),
                  PropertyType(propName("p-3"), FloatType(isNullable = true)(pos)),
                  PropertyType(propName("p%1"), IntegerType(isNullable = true)(pos))
                )
              ),
              NodeElementType(
                label("L-1"),
                Set(label("L-3"), label("L-4"), label("L-2")),
                Set(
                  PropertyType(propName("p`2"), BooleanType(isNullable = true)(pos)),
                  PropertyType(propName("p`1"), StringType(isNullable = false)(pos))
                )
              ),
              NodeElementType(
                label("L-5"),
                Set.empty,
                Set(
                  PropertyType(propName("p-2"), AnyType(isNullable = false)(pos)),
                  PropertyType(propName("p-1"), IntegerType(isNullable = true)(pos))
                )
              ),
              RelationshipElementType(
                relType("R%1"),
                EmptyNodeElementTypeReference,
                NodeElementTypeReferenceByLabel(label("L%5")),
                Set.empty
              )
            ),
            Set(
              GraphTypeCreateConstraint(
                Some("c-2"),
                RelationshipElementTypeReferenceByLabel(relType("R-1")),
                ArraySeq(propName("p-2"), propName("p-1")),
                KeyConstraint,
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByLabel(label("L%3")),
                ArraySeq(propName("p`2")),
                PropertyTypeConstraint(IntegerType(isNullable = true)(pos)),
                NoOptions
              ),
              GraphTypeCreateConstraint(
                None,
                NodeElementTypeReferenceByIdentifyingLabel(label("L!2")),
                ArraySeq(propName("2p2"), propName("3p3"), propName("1p1")),
                UniquenessConstraint,
                OptionsParam(parameter("par\nam", CTMap))(pos)
              ),
              GraphTypeCreateConstraint(
                Some("c-3"),
                RelationshipElementTypeReferenceByLabel(relType("R`3")),
                ArraySeq(propName("p-1")),
                ExistenceConstraint,
                NoOptions
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAdd",
        Seq.empty,
        Seq(details(
          "{ (:`L-1` => :`L-2`&`L-3`&`L-4` {`p``1` :: STRING NOT NULL, `p``2` :: BOOLEAN}), " +
            "(:`L-5` => {`p-1` :: INTEGER, `p-2` :: ANY NOT NULL}), " +
            "()-[:`R%1` =>]->(:`L%5`), " +
            "(:`L!2`)-[:`R-2` => {`1p2` :: DATE, `p%1` :: INTEGER, `p-3` :: FLOAT}]->(:`L``1` =>), " +
            "CONSTRAINT FOR (n:`L!2` =>) REQUIRE (n.`2p2`, n.`3p3`, n.`1p1`) IS UNIQUE OPTIONS $`par am`, " +
            "CONSTRAINT FOR (n:`L%3`) REQUIRE n.`p``2` IS :: INTEGER, " +
            "CONSTRAINT `c-2` FOR ()-[r:`R-1`]->() REQUIRE (r.`p-2`, r.`p-1`) IS KEY, " +
            "CONSTRAINT `c-3` FOR ()-[r:`R``3`]->() REQUIRE r.`p-1` IS NOT NULL }"
        )),
        Set.empty
      )
    )
  }

  test("AlterCurrentGraphType - drop, empty graph type") {
    assertGood(
      attach(
        AlterCurrentGraphType(GraphTypeForDrop(Set.empty, Set.empty)),
        1.0
      ),
      planDescription(id, "AlterCurrentGraphTypeDrop", Seq.empty, Seq(details("{}")), Set.empty)
    )
  }

  test("AlterCurrentGraphType - drop, node and rel elem types") {
    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForDrop(
            Set(
              NodeElementType(
                label("L"),
                Set.empty,
                Set(PropertyType(propName("p"), IntegerType(isNullable = true)(pos)))
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(id, "AlterCurrentGraphTypeDrop", Seq.empty, Seq(details("{ (:L => {p :: INTEGER}) }")), Set.empty)
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForDrop(
            Set(
              NodeElementType(
                label("L1"),
                Set(label("L2")),
                Set.empty
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(id, "AlterCurrentGraphTypeDrop", Seq.empty, Seq(details("{ (:L1 => :L2) }")), Set.empty)
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForDrop(
            Set(
              NodeElementType(
                label("L1"),
                Set(label("L3"), label("L2")),
                Set(
                  PropertyType(propName("p1"), IntegerType(isNullable = false)(pos)),
                  PropertyType(propName("p2"), BooleanType(isNullable = true)(pos))
                )
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeDrop",
        Seq.empty,
        Seq(details("{ (:L1 => :L2&L3 {p1 :: INTEGER NOT NULL, p2 :: BOOLEAN}) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForDrop(
            Set(
              RelationshipElementType(
                relType("R"),
                EmptyNodeElementTypeReference,
                EmptyNodeElementTypeReference,
                Set(PropertyType(propName("p"), IntegerType(isNullable = true)(pos)))
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeDrop",
        Seq.empty,
        Seq(details("{ ()-[:R => {p :: INTEGER}]->() }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForDrop(
            Set(
              RelationshipElementType(
                relType("R"),
                EmptyNodeElementTypeReference,
                EmptyNodeElementTypeReference,
                Set(PropertyType(
                  propName("p"),
                  VectorType(Some(Float32Type(isNullable = false)(pos)), Some(75), isNullable = true)(pos)
                ))
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeDrop",
        Seq.empty,
        Seq(details("{ ()-[:R => {p :: VECTOR<FLOAT32 NOT NULL>(75)}]->() }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForDrop(
            Set(
              RelationshipElementType(
                relType("R"),
                NodeElementTypeReferenceByIdentifyingLabel(label("L2")),
                NodeElementTypeReferenceByLabel(label("L1")),
                Set.empty
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeDrop",
        Seq.empty,
        Seq(details("{ (:L2 =>)-[:R =>]->(:L1) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForDrop(
            Set(
              RelationshipElementType(
                relType("R"),
                NodeElementTypeReferenceByIdentifyingLabel(label("L")),
                EmptyNodeElementTypeReference,
                Set.empty
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeDrop",
        Seq.empty,
        Seq(details("{ (:L =>)-[:R =>]->() }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForDrop(
            Set(
              RelationshipElementType(
                relType("R"),
                EmptyNodeElementTypeReference,
                NodeElementTypeReferenceByLabel(label("L")),
                Set.empty
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeDrop",
        Seq.empty,
        Seq(details("{ ()-[:R =>]->(:L) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForDrop(
            Set(
              RelationshipElementType(
                relType("R"),
                EmptyNodeElementTypeReference,
                NodeElementTypeReferenceByIdentifyingLabel(label("L")),
                Set(
                  PropertyType(propName("p2"), DateType(isNullable = false)(pos)),
                  PropertyType(propName("p1"), StringType(isNullable = false)(pos))
                )
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeDrop",
        Seq.empty,
        Seq(details("{ ()-[:R => {p1 :: STRING NOT NULL, p2 :: DATE NOT NULL}]->(:L =>) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForDrop(
            Set(
              RelationshipElementType(
                relType("R"),
                NodeElementTypeReferenceByLabel(label("L1")),
                NodeElementTypeReferenceByIdentifyingLabel(label("L2")),
                Set(PropertyType(propName("p"), IntegerType(isNullable = true)(pos)))
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeDrop",
        Seq.empty,
        Seq(details("{ (:L1)-[:R => {p :: INTEGER}]->(:L2 =>) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForDrop(
            Set(
              RelationshipElementType(
                relType("R2"),
                NodeElementTypeReferenceByLabel(label("L2")),
                NodeElementTypeReferenceByIdentifyingLabel(label("L1")),
                Set(
                  PropertyType(propName("p2"), DateType(isNullable = true)(pos)),
                  PropertyType(propName("p3"), FloatType(isNullable = true)(pos)),
                  PropertyType(propName("p1"), IntegerType(isNullable = true)(pos))
                )
              ),
              NodeElementType(
                label("L1"),
                Set(label("L3"), label("L4"), label("L2")),
                Set(
                  PropertyType(propName("p2"), BooleanType(isNullable = true)(pos)),
                  PropertyType(propName("p1"), StringType(isNullable = false)(pos))
                )
              ),
              NodeElementType(
                label("L5"),
                Set.empty,
                Set(
                  PropertyType(propName("p2"), AnyType(isNullable = false)(pos)),
                  PropertyType(propName("p1"), IntegerType(isNullable = true)(pos))
                )
              ),
              RelationshipElementType(
                relType("R1"),
                EmptyNodeElementTypeReference,
                NodeElementTypeReferenceByLabel(label("L5")),
                Set.empty
              )
            ),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeDrop",
        Seq.empty,
        Seq(details(
          "{ (:L1 => :L2&L3&L4 {p1 :: STRING NOT NULL, p2 :: BOOLEAN}), (:L5 => {p1 :: INTEGER, p2 :: ANY NOT NULL}), " +
            "()-[:R1 =>]->(:L5), (:L2)-[:R2 => {p1 :: INTEGER, p2 :: DATE, p3 :: FLOAT}]->(:L1 =>) }"
        )),
        Set.empty
      )
    )
  }

  test("AlterCurrentGraphType - drop, constraints") {
    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForDrop(
            Set.empty,
            Set(GraphTypeDropConstraint("const"))
          )
        ),
        1.0
      ),
      planDescription(id, "AlterCurrentGraphTypeDrop", Seq.empty, Seq(details("{ CONSTRAINT const }")), Set.empty)
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForDrop(
            Set.empty,
            Set(GraphTypeDropConstraint("con-st"))
          )
        ),
        1.0
      ),
      planDescription(id, "AlterCurrentGraphTypeDrop", Seq.empty, Seq(details("{ CONSTRAINT `con-st` }")), Set.empty)
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForDrop(
            Set.empty,
            Set(GraphTypeDropConstraint("con\nst"))
          )
        ),
        1.0
      ),
      planDescription(id, "AlterCurrentGraphTypeDrop", Seq.empty, Seq(details("{ CONSTRAINT `con st` }")), Set.empty)
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForDrop(
            Set.empty,
            Set(
              GraphTypeDropConstraint("const2"),
              GraphTypeDropConstraint("const4"),
              GraphTypeDropConstraint("const1"),
              GraphTypeDropConstraint("const3")
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeDrop",
        Seq.empty,
        Seq(details(
          "{ CONSTRAINT const1, CONSTRAINT const2, CONSTRAINT const3, CONSTRAINT const4 }"
        )),
        Set.empty
      )
    )
  }

  test("AlterCurrentGraphType - drop, mixed") {
    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForDrop(
            Set(
              RelationshipElementType(
                relType("R-2"),
                NodeElementTypeReferenceByLabel(label("L!2")),
                NodeElementTypeReferenceByIdentifyingLabel(label("L`1")),
                Set(
                  PropertyType(propName("1p2"), DateType(isNullable = true)(pos)),
                  PropertyType(propName("p-3"), FloatType(isNullable = true)(pos)),
                  PropertyType(propName("p%1"), IntegerType(isNullable = true)(pos))
                )
              ),
              NodeElementType(
                label("L-1"),
                Set(label("L-3"), label("L-4"), label("L-2")),
                Set(
                  PropertyType(propName("p`2"), BooleanType(isNullable = true)(pos)),
                  PropertyType(propName("p`1"), StringType(isNullable = false)(pos))
                )
              ),
              NodeElementType(
                label("L-5"),
                Set.empty,
                Set(
                  PropertyType(propName("p-2"), AnyType(isNullable = false)(pos)),
                  PropertyType(propName("p-1"), IntegerType(isNullable = true)(pos))
                )
              ),
              RelationshipElementType(
                relType("R%1"),
                EmptyNodeElementTypeReference,
                NodeElementTypeReferenceByLabel(label("L%5")),
                Set.empty
              )
            ),
            Set(
              GraphTypeDropConstraint("const5"),
              GraphTypeDropConstraint("1const"),
              GraphTypeDropConstraint("con`st")
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeDrop",
        Seq.empty,
        Seq(details(
          "{ (:`L-1` => :`L-2`&`L-3`&`L-4` {`p``1` :: STRING NOT NULL, `p``2` :: BOOLEAN}), " +
            "(:`L-5` => {`p-1` :: INTEGER, `p-2` :: ANY NOT NULL}), " +
            "()-[:`R%1` =>]->(:`L%5`), " +
            "(:`L!2`)-[:`R-2` => {`1p2` :: DATE, `p%1` :: INTEGER, `p-3` :: FLOAT}]->(:`L``1` =>), " +
            "CONSTRAINT `1const`, " +
            "CONSTRAINT `con``st`, " +
            "CONSTRAINT const5 }"
        )),
        Set.empty
      )
    )
  }

  test("AlterCurrentGraphType - alter, empty graph type") {
    assertGood(
      attach(
        AlterCurrentGraphType(GraphTypeForAlter(Set.empty)),
        1.0
      ),
      planDescription(id, "AlterCurrentGraphTypeAlter", Seq.empty, Seq(details("{}")), Set.empty)
    )
  }

  test("AlterCurrentGraphType - alter, node and rel elem types") {
    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAlter(
            Set(
              NodeElementType(
                label("L"),
                Set.empty,
                Set(PropertyType(propName("p"), IntegerType(isNullable = true)(pos)))
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAlter",
        Seq.empty,
        Seq(details("{ (:L => {p :: INTEGER}) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAlter(
            Set(
              NodeElementType(
                label("L1"),
                Set(label("L2")),
                Set.empty
              )
            )
          )
        ),
        1.0
      ),
      planDescription(id, "AlterCurrentGraphTypeAlter", Seq.empty, Seq(details("{ (:L1 => :L2) }")), Set.empty)
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAlter(
            Set(
              NodeElementType(
                label("L1"),
                Set(label("L3"), label("L2")),
                Set(
                  PropertyType(propName("p1"), IntegerType(isNullable = false)(pos)),
                  PropertyType(propName("p2"), BooleanType(isNullable = true)(pos))
                )
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAlter",
        Seq.empty,
        Seq(details("{ (:L1 => :L2&L3 {p1 :: INTEGER NOT NULL, p2 :: BOOLEAN}) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAlter(
            Set(
              RelationshipElementType(
                relType("R"),
                EmptyNodeElementTypeReference,
                EmptyNodeElementTypeReference,
                Set(PropertyType(propName("p"), IntegerType(isNullable = true)(pos)))
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAlter",
        Seq.empty,
        Seq(details("{ ()-[:R => {p :: INTEGER}]->() }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAlter(
            Set(
              RelationshipElementType(
                relType("R"),
                NodeElementTypeReferenceByIdentifyingLabel(label("L2")),
                NodeElementTypeReferenceByLabel(label("L1")),
                Set.empty
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAlter",
        Seq.empty,
        Seq(details("{ (:L2 =>)-[:R =>]->(:L1) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAlter(
            Set(
              RelationshipElementType(
                relType("R"),
                NodeElementTypeReferenceByIdentifyingLabel(label("L")),
                EmptyNodeElementTypeReference,
                Set.empty
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAlter",
        Seq.empty,
        Seq(details("{ (:L =>)-[:R =>]->() }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAlter(
            Set(
              RelationshipElementType(
                relType("R"),
                EmptyNodeElementTypeReference,
                NodeElementTypeReferenceByLabel(label("L")),
                Set.empty
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAlter",
        Seq.empty,
        Seq(details("{ ()-[:R =>]->(:L) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAlter(
            Set(
              RelationshipElementType(
                relType("R"),
                EmptyNodeElementTypeReference,
                NodeElementTypeReferenceByIdentifyingLabel(label("L")),
                Set(
                  PropertyType(propName("p2"), DateType(isNullable = false)(pos)),
                  PropertyType(propName("p1"), StringType(isNullable = false)(pos))
                )
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAlter",
        Seq.empty,
        Seq(details("{ ()-[:R => {p1 :: STRING NOT NULL, p2 :: DATE NOT NULL}]->(:L =>) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAlter(
            Set(
              RelationshipElementType(
                relType("R"),
                NodeElementTypeReferenceByLabel(label("L1")),
                NodeElementTypeReferenceByIdentifyingLabel(label("L2")),
                Set(PropertyType(propName("p"), IntegerType(isNullable = true)(pos)))
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAlter",
        Seq.empty,
        Seq(details("{ (:L1)-[:R => {p :: INTEGER}]->(:L2 =>) }")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAlter(
            Set(
              RelationshipElementType(
                relType("R2"),
                NodeElementTypeReferenceByLabel(label("L2")),
                NodeElementTypeReferenceByIdentifyingLabel(label("L1")),
                Set(
                  PropertyType(propName("p2"), DateType(isNullable = true)(pos)),
                  PropertyType(propName("p3"), FloatType(isNullable = true)(pos)),
                  PropertyType(propName("p1"), IntegerType(isNullable = true)(pos))
                )
              ),
              NodeElementType(
                label("L1"),
                Set(label("L3"), label("L4"), label("L2")),
                Set(
                  PropertyType(propName("p2"), BooleanType(isNullable = true)(pos)),
                  PropertyType(propName("p1"), StringType(isNullable = false)(pos))
                )
              ),
              NodeElementType(
                label("L5"),
                Set.empty,
                Set(
                  PropertyType(propName("p2"), AnyType(isNullable = false)(pos)),
                  PropertyType(propName("p1"), IntegerType(isNullable = true)(pos))
                )
              ),
              RelationshipElementType(
                relType("R1"),
                EmptyNodeElementTypeReference,
                NodeElementTypeReferenceByLabel(label("L5")),
                Set.empty
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAlter",
        Seq.empty,
        Seq(details(
          "{ (:L1 => :L2&L3&L4 {p1 :: STRING NOT NULL, p2 :: BOOLEAN}), (:L5 => {p1 :: INTEGER, p2 :: ANY NOT NULL}), " +
            "()-[:R1 =>]->(:L5), (:L2)-[:R2 => {p1 :: INTEGER, p2 :: DATE, p3 :: FLOAT}]->(:L1 =>) }"
        )),
        Set.empty
      )
    )

    assertGood(
      attach(
        AlterCurrentGraphType(
          GraphTypeForAlter(
            Set(
              RelationshipElementType(
                relType("R-2"),
                NodeElementTypeReferenceByLabel(label("L!2")),
                NodeElementTypeReferenceByIdentifyingLabel(label("L`1")),
                Set(
                  PropertyType(propName("1p2"), DateType(isNullable = true)(pos)),
                  PropertyType(propName("p-3"), FloatType(isNullable = true)(pos)),
                  PropertyType(propName("p%1"), IntegerType(isNullable = true)(pos))
                )
              ),
              NodeElementType(
                label("L-1"),
                Set(label("L-3"), label("L-4"), label("L-2")),
                Set(
                  PropertyType(propName("p`2"), BooleanType(isNullable = true)(pos)),
                  PropertyType(propName("p`1"), StringType(isNullable = false)(pos))
                )
              ),
              NodeElementType(
                label("L-5"),
                Set.empty,
                Set(
                  PropertyType(propName("p-2"), AnyType(isNullable = false)(pos)),
                  PropertyType(propName("p-1"), IntegerType(isNullable = true)(pos))
                )
              ),
              RelationshipElementType(
                relType("R%1"),
                EmptyNodeElementTypeReference,
                NodeElementTypeReferenceByLabel(label("L%5")),
                Set.empty
              )
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "AlterCurrentGraphTypeAlter",
        Seq.empty,
        Seq(details(
          "{ (:`L-1` => :`L-2`&`L-3`&`L-4` {`p``1` :: STRING NOT NULL, `p``2` :: BOOLEAN}), " +
            "(:`L-5` => {`p-1` :: INTEGER, `p-2` :: ANY NOT NULL}), " +
            "()-[:`R%1` =>]->(:`L%5`), " +
            "(:`L!2`)-[:`R-2` => {`1p2` :: DATE, `p%1` :: INTEGER, `p-3` :: FLOAT}]->(:`L``1` =>) }"
        )),
        Set.empty
      )
    )
  }

  test("ShowCurrentGraphType") {
    assertGood(
      attach(
        ShowCurrentGraphType(asGraph = false, List.empty, List.empty, yieldAll = false, Set.empty, Set.empty),
        1.0
      ),
      planDescription(
        id,
        "ShowCurrentGraphType",
        Seq.empty,
        Seq(details("graphTypeAsString, defaultColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowCurrentGraphType(asGraph = false, List.empty, List.empty, yieldAll = true, Set.empty, Set.empty),
        1.0
      ),
      planDescription(id, "ShowCurrentGraphType", Seq.empty, Seq(details("graphTypeAsString, allColumns")), Set.empty)
    )

    assertGood(
      attach(
        ShowCurrentGraphType(
          asGraph = true,
          List(commandDefaultColumn("xxx"), commandDefaultColumn("yyy"), commandDefaultColumn("vvv")),
          List(
            CommandYieldColumn("xxx", "xxx"),
            CommandYieldColumn("yyy", "zzz"),
            CommandYieldColumn("vvv", "vvv")
          ),
          yieldAll = false,
          Set(varFor("xxx"), varFor("zzz"), varFor("vvv")),
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowCurrentGraphType",
        Seq.empty,
        Seq(details("graphTypeAsGraph, columns(xxx, yyy AS zzz, vvv)")),
        Set("xxx", "zzz", "vvv")
      )
    )
  }

  // Remaining non-admin show and terminate commands

  test("ShowProcedures") {
    assertGood(
      attach(ShowProcedures(None, List.empty, List.empty, yieldAll = false, Set.empty, Set.empty), 1.0),
      planDescription(
        id,
        "ShowProcedures",
        Seq.empty,
        Seq(details("proceduresForUser(all), defaultColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(ShowProcedures(Some(CurrentUser), List.empty, List.empty, yieldAll = true, Set.empty, Set.empty), 1.0),
      planDescription(
        id,
        "ShowProcedures",
        Seq.empty,
        Seq(details("proceduresForUser(current), allColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowProcedures(
          Some(User("foo")(pos)),
          List(commandDefaultColumn("xxx"), commandDefaultColumn("yyy"), commandDefaultColumn("vvv")),
          List(
            CommandYieldColumn("xxx", "xxx"),
            CommandYieldColumn("yyy", "zzz"),
            CommandYieldColumn("vvv", "vvv")
          ),
          yieldAll = false,
          Set(varFor("xxx"), varFor("zzz"), varFor("vvv")),
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowProcedures",
        Seq.empty,
        Seq(details("proceduresForUser(foo), columns(xxx, yyy AS zzz, vvv)")),
        Set("xxx", "zzz", "vvv")
      )
    )
  }

  test("ShowFunctions") {
    assertGood(
      attach(ShowFunctions(AllFunctions, None, List.empty, List.empty, yieldAll = false, Set.empty, Set.empty), 1.0),
      planDescription(
        id,
        "ShowFunctions",
        Seq.empty,
        Seq(details("allFunctions, functionsForUser(all), defaultColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowFunctions(
          BuiltInFunctions,
          Some(CurrentUser),
          List.empty,
          List.empty,
          yieldAll = true,
          Set.empty,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowFunctions",
        Seq.empty,
        Seq(details("builtInFunctions, functionsForUser(current), allColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowFunctions(
          UserDefinedFunctions,
          Some(User("foo")(pos)),
          List(commandDefaultColumn("xxx"), commandDefaultColumn("yyy"), commandDefaultColumn("vvv")),
          List(
            CommandYieldColumn("xxx", "xxx"),
            CommandYieldColumn("yyy", "zzz"),
            CommandYieldColumn("vvv", "vvv")
          ),
          yieldAll = false,
          Set(varFor("xxx"), varFor("zzz"), varFor("vvv")),
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowFunctions",
        Seq.empty,
        Seq(details("userDefinedFunctions, functionsForUser(foo), columns(xxx, yyy AS zzz, vvv)")),
        Set("xxx", "zzz", "vvv")
      )
    )
  }

  test("ShowSettings") {
    val defaultColumns = List(commandDefaultColumn("xxx"), commandDefaultColumn("yyy"))
    val defaultVariables: Set[LogicalVariable] = Set(varFor("xxx"), varFor("yyy"))

    assertGood(
      attach(
        ShowSettings(
          NoNames,
          defaultColumns,
          List.empty,
          yieldAll = true,
          defaultVariables,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowSettings",
        Seq.empty,
        Seq(details("allSettings, allColumns")),
        Set("xxx", "yyy")
      )
    )

    assertGood(
      attach(
        ShowSettings(
          CommaSeparatedNames(listOfString("Foo", "Bar")),
          defaultColumns,
          List.empty,
          yieldAll = false,
          defaultVariables,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowSettings",
        Seq.empty,
        Seq(details("settings(\"Foo\", \"Bar\"), defaultColumns")),
        Set("xxx", "yyy")
      )
    )

    assertGood(
      attach(
        ShowSettings(
          ExpressionNames(stringLiteral("foo.*")),
          List(commandDefaultColumn("xxx"), commandDefaultColumn("yyy"), commandDefaultColumn("vvv")),
          List(
            CommandYieldColumn("xxx", "xxx"),
            CommandYieldColumn("yyy", "zzz"),
            CommandYieldColumn("vvv", "vvv")
          ),
          yieldAll = false,
          Set(varFor("xxx"), varFor("zzz"), varFor("vvv")),
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowSettings",
        Seq.empty,
        Seq(details("settings(\"foo.*\"), columns(xxx, yyy AS zzz, vvv)")),
        Set("xxx", "zzz", "vvv")
      )
    )
  }

  test("ShowTransactions") {
    val defaultColumns = List(commandDefaultColumn("xxx"), commandDefaultColumn("yyy"))
    val defaultVariables: Set[LogicalVariable] = Set(varFor("xxx"), varFor("yyy"))

    assertGood(
      attach(
        ShowTransactions(
          NoNames,
          defaultColumns,
          List.empty,
          yieldAll = false,
          defaultVariables,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowTransactions",
        Seq.empty,
        Seq(details("defaultColumns, allTransactions")),
        Set("xxx", "yyy")
      )
    )

    assertGood(
      attach(
        ShowTransactions(
          CommaSeparatedNames(listOfString("db1-transaction-123")),
          defaultColumns,
          List.empty,
          yieldAll = true,
          defaultVariables,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowTransactions",
        Seq.empty,
        Seq(details("allColumns, transactions(\"db1-transaction-123\")")),
        Set("xxx", "yyy")
      )
    )

    assertGood(
      attach(
        ShowTransactions(
          CommaSeparatedNames(listOfString("db1-transaction-123", "db2-transaction-456")),
          List(commandDefaultColumn("xxx"), commandDefaultColumn("yyy"), commandDefaultColumn("vvv")),
          List(
            CommandYieldColumn("xxx", "xxx"),
            CommandYieldColumn("yyy", "zzz"),
            CommandYieldColumn("vvv", "vvv")
          ),
          yieldAll = false,
          Set(varFor("xxx"), varFor("zzz"), varFor("vvv")),
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowTransactions",
        Seq.empty,
        Seq(details("columns(xxx, yyy AS zzz, vvv), transactions(\"db1-transaction-123\", \"db2-transaction-456\")")),
        Set("xxx", "zzz", "vvv")
      )
    )

    assertGood(
      attach(
        ShowTransactions(
          ExpressionNames(parameter("foo", CTAny)),
          defaultColumns,
          List.empty,
          yieldAll = false,
          defaultVariables,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowTransactions",
        Seq.empty,
        Seq(details("defaultColumns, transactions($foo)")),
        Set("xxx", "yyy")
      )
    )

    assertGood(
      attach(
        ShowTransactions(
          ExpressionNames(varFor("foo")),
          defaultColumns,
          List.empty,
          yieldAll = false,
          defaultVariables,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowTransactions",
        Seq.empty,
        Seq(details("defaultColumns, transactions(foo)")),
        Set("xxx", "yyy")
      )
    )

    assertGood(
      attach(
        ShowTransactions(
          ExpressionNames(Add(varFor("foo"), stringLiteral("123"))(pos)),
          defaultColumns,
          List.empty,
          yieldAll = false,
          defaultVariables,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowTransactions",
        Seq.empty,
        Seq(details("defaultColumns, transactions(foo + \"123\")")),
        Set("xxx", "yyy")
      )
    )
  }

  test("TerminateTransactions") {
    val defaultColumns = List(commandDefaultColumn("xxx"), commandDefaultColumn("yyy"))
    val defaultVariables: Set[LogicalVariable] = Set(varFor("xxx"), varFor("yyy"))

    assertGood(
      attach(
        TerminateTransactions(
          CommaSeparatedNames(listOfString("db1-transaction-123")),
          defaultColumns,
          List.empty,
          yieldAll = false,
          defaultVariables,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "TerminateTransactions",
        Seq.empty,
        Seq(details("defaultColumns, transactions(\"db1-transaction-123\")")),
        Set("xxx", "yyy")
      )
    )

    assertGood(
      attach(
        TerminateTransactions(
          CommaSeparatedNames(listOfString("db1-transaction-123", "db2-transaction-456")),
          defaultColumns,
          List(CommandYieldColumn("xxx", "xxx"), CommandYieldColumn("yyy", "zzz")),
          yieldAll = false,
          Set(varFor("xxx"), varFor("zzz")),
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "TerminateTransactions",
        Seq.empty,
        Seq(details("columns(xxx, yyy AS zzz), transactions(\"db1-transaction-123\", \"db2-transaction-456\")")),
        Set("xxx", "zzz")
      )
    )

    assertGood(
      attach(
        TerminateTransactions(
          ExpressionNames(parameter("foo", CTAny)),
          defaultColumns,
          List.empty,
          yieldAll = true,
          defaultVariables,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "TerminateTransactions",
        Seq.empty,
        Seq(details("allColumns, transactions($foo)")),
        Set("xxx", "yyy")
      )
    )

    assertGood(
      attach(
        TerminateTransactions(
          ExpressionNames(number("123")),
          defaultColumns,
          List.empty,
          yieldAll = true,
          defaultVariables,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "TerminateTransactions",
        Seq.empty,
        Seq(details("allColumns, transactions(123)")),
        Set("xxx", "yyy")
      )
    )
  }

  test("ShowDatabases") {
    val defaultColumns = List(commandDefaultColumn("xxx"), commandDefaultColumn("yyy"))
    val defaultVariables: Set[LogicalVariable] = Set(varFor("xxx"), varFor("yyy"))

    assertGood(
      attach(
        ShowDatabases(
          AllDatabasesScope()(pos),
          defaultColumns,
          List.empty,
          yieldAll = true,
          defaultVariables,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowDatabases",
        Seq.empty,
        Seq(details("allDatabases, allColumns")),
        Set("xxx", "yyy")
      )
    )

    assertGood(
      attach(
        ShowDatabases(
          HomeDatabaseScope()(pos),
          defaultColumns,
          List.empty,
          yieldAll = false,
          defaultVariables,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowDatabases",
        Seq.empty,
        Seq(details("homeDatabase, defaultColumns")),
        Set("xxx", "yyy")
      )
    )

    assertGood(
      attach(
        ShowDatabases(
          DefaultDatabaseScope()(pos),
          defaultColumns,
          List.empty,
          yieldAll = true,
          defaultVariables,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowDatabases",
        Seq.empty,
        Seq(details("defaultDatabase, allColumns")),
        Set("xxx", "yyy")
      )
    )

    assertGood(
      attach(
        ShowDatabases(
          SingleNamedDatabaseScope(NamespacedName("neo4j")(pos))(pos),
          defaultColumns,
          List(CommandYieldColumn("xxx", "xxx"), CommandYieldColumn("yyy", "zzz")),
          yieldAll = false,
          defaultVariables,
          Set.empty
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowDatabases",
        Seq.empty,
        Seq(details("database(neo4j), columns(xxx, yyy AS zzz)")),
        Set("xxx", "yyy")
      )
    )
  }

  // Combining commands

  test("Multiple show/terminate commands") {

    assertGood(
      attach(
        Apply(
          ShowTransactions(
            CommaSeparatedNames(listOfString("db1-transaction-123", "db2-transaction-456")),
            List(commandDefaultColumn("xxx"), commandDefaultColumn("yyy"), commandDefaultColumn("vvv")),
            List(
              CommandYieldColumn("xxx", "xxx"),
              CommandYieldColumn("yyy", "zzz"),
              CommandYieldColumn("vvv", "vvv")
            ),
            yieldAll = false,
            Set(varFor("xxx"), varFor("zzz"), varFor("vvv")),
            Set.empty
          ),
          TerminateTransactions(
            ExpressionNames(parameter("foo", CTAny)),
            List(commandDefaultColumn("xxx"), commandDefaultColumn("yyy")),
            List.empty,
            yieldAll = true,
            Set(varFor("xxx"), varFor("yyy")),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "Apply",
        Seq(
          planDescription(
            id,
            "ShowTransactions",
            Seq.empty,
            Seq(
              details("columns(xxx, yyy AS zzz, vvv), transactions(\"db1-transaction-123\", \"db2-transaction-456\")")
            ),
            Set("xxx", "zzz", "vvv")
          ),
          planDescription(
            id,
            "TerminateTransactions",
            Seq.empty,
            Seq(details("allColumns, transactions($foo)")),
            Set("xxx", "yyy")
          )
        ),
        Seq.empty,
        Set("xxx", "zzz", "vvv", "yyy")
      )
    )

    assertGood(
      attach(
        Apply(
          ShowDatabases(
            SingleNamedDatabaseScope(NamespacedName("foo")(pos))(pos),
            List(commandDefaultColumn("xxx"), commandDefaultColumn("yyy"), commandDefaultColumn("vvv")),
            List(
              CommandYieldColumn("xxx", "xxx"),
              CommandYieldColumn("yyy", "zzz"),
              CommandYieldColumn("vvv", "vvv")
            ),
            yieldAll = false,
            Set(varFor("xxx"), varFor("zzz"), varFor("vvv")),
            Set.empty
          ),
          ShowSettings(
            ExpressionNames(parameter("foo", CTAny)),
            List(commandDefaultColumn("xxx"), commandDefaultColumn("yyy")),
            List.empty,
            yieldAll = true,
            Set(varFor("xxx"), varFor("yyy")),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "Apply",
        Seq(
          planDescription(
            id,
            "ShowDatabases",
            Seq.empty,
            Seq(details("database(foo), columns(xxx, yyy AS zzz, vvv)")),
            Set("xxx", "zzz", "vvv")
          ),
          planDescription(
            id,
            "ShowSettings",
            Seq.empty,
            Seq(details("settings($foo), allColumns")),
            Set("xxx", "yyy")
          )
        ),
        Seq.empty,
        Set("xxx", "zzz", "vvv", "yyy")
      )
    )

  }

  test("Show/terminate commands together with regular Cypher") {
    val name = procedureName("my", "proc", "foo")
    val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
    val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))
    val signature =
      ProcedureSignature(name, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess, id = 42)
    val callResults = IndexedSeq(ProcedureResultItem(varFor("x"))(pos), ProcedureResultItem(varFor("y"))(pos))
    val call = ResolvedNonLocalCall(signature, Seq(varFor("a1")), callResults)(pos)

    assertGood(
      attach(
        ProduceResult.withNoCachedProperties(
          ProcedureCall(
            ShowFunctions(AllFunctions, None, List.empty, List.empty, yieldAll = false, Set.empty, Set.empty),
            call
          ),
          Seq(varFor("a"), varFor("b"), varFor("c\nd"))
        ),
        1.0
      ),
      planDescription(
        id,
        "ProduceResults",
        Seq(
          planDescription(
            id,
            "ProcedureCall",
            Seq(
              planDescription(
                id,
                "ShowFunctions",
                Seq.empty,
                Seq(details("allFunctions, functionsForUser(all), defaultColumns")),
                Set.empty
              )
            ),
            Seq(details("my.proc.foo(a1) :: (x :: INTEGER, y :: LIST<NODE>)")),
            Set("x", "y")
          )
        ),
        Seq(details(Seq("a", "b", "`c d`"))),
        Set("x", "y")
      )
    )

    assertGood(
      attach(
        ProduceResult.withNoCachedProperties(
          Projection(
            UnwindCollection(
              ShowConstraints(
                constraintType = RelPropTypeConstraints,
                List(commandDefaultColumn("xxx"), commandDefaultColumn("yyy"), commandDefaultColumn("vvv")),
                List(
                  CommandYieldColumn("xxx", "xxx"),
                  CommandYieldColumn("yyy", "zzz"),
                  CommandYieldColumn("vvv", "vvv")
                ),
                yieldAll = false,
                Set(varFor("xxx"), varFor("zzz"), varFor("vvv")),
                Set.empty
              ),
              varFor("x"),
              varFor("list")
            ),
            Map(varFor("x") -> varFor("y"))
          ),
          Seq(varFor("a"), varFor("b"), varFor("c\nd"))
        ),
        1.0
      ),
      planDescription(
        id,
        "ProduceResults",
        Seq(
          planDescription(
            id,
            "Projection",
            Seq(
              planDescription(
                id,
                "Unwind",
                Seq(
                  planDescription(
                    id,
                    "ShowConstraints",
                    Seq.empty,
                    Seq(details("relationshipPropertyTypeConstraints, columns(xxx, yyy AS zzz, vvv)")),
                    Set("xxx", "zzz", "vvv")
                  )
                ),
                Seq(details("list AS x")),
                Set("xxx", "zzz", "vvv", "x")
              )
            ),
            Seq(details("y AS x")),
            Set("xxx", "zzz", "vvv", "x")
          )
        ),
        Seq(details(Seq("a", "b", "`c d`"))),
        Set("xxx", "zzz", "vvv", "x")
      )
    )

    assertGood(
      attach(
        Union(
          ShowProcedures(None, List.empty, List.empty, yieldAll = false, Set.empty, Set.empty),
          ShowSettings(
            CommaSeparatedNames(listOfString("Foo", "Bar")),
            List(commandDefaultColumn("xxx"), commandDefaultColumn("yyy")),
            List.empty,
            yieldAll = false,
            Set(varFor("xxx"), varFor("yyy")),
            Set.empty
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "Union",
        Seq(
          planDescription(
            id,
            "ShowProcedures",
            Seq.empty,
            Seq(details("proceduresForUser(all), defaultColumns")),
            Set.empty
          ),
          planDescription(
            id,
            "ShowSettings",
            Seq.empty,
            Seq(details("settings(\"Foo\", \"Bar\"), defaultColumns")),
            Set("xxx", "yyy")
          )
        ),
        Seq.empty,
        Set.empty
      )
    )

    assertGood(
      attach(
        ProduceResult.withNoCachedProperties(
          ProcedureCall(
            ShowDatabases(AllDatabasesScope()(pos), List.empty, List.empty, yieldAll = false, Set.empty, Set.empty),
            call
          ),
          Seq(varFor("a"), varFor("b"), varFor("c\nd"))
        ),
        1.0
      ),
      planDescription(
        id,
        "ProduceResults",
        Seq(
          planDescription(
            id,
            "ProcedureCall",
            Seq(
              planDescription(
                id,
                "ShowDatabases",
                Seq.empty,
                Seq(details("allDatabases, defaultColumns")),
                Set.empty
              )
            ),
            Seq(details("my.proc.foo(a1) :: (x :: INTEGER, y :: LIST<NODE>)")),
            Set("x", "y")
          )
        ),
        Seq(details(Seq("a", "b", "`c d`"))),
        Set("x", "y")
      )
    )
  }

  // Help methods

  private def commandDefaultColumn(name: String) = CommandDefaultColumn(name, CTString)

}
