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
package org.neo4j.cypher.internal.physicalplanning.ast.prettifier

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationBuilder
import org.neo4j.cypher.internal.physicalplanning.ast.GetDegreePrimitive
import org.neo4j.cypher.internal.physicalplanning.ast.HasALabelFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.HasAnyLabelFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.HasDegreeGreaterThanOrEqualPrimitive
import org.neo4j.cypher.internal.physicalplanning.ast.HasDegreeGreaterThanPrimitive
import org.neo4j.cypher.internal.physicalplanning.ast.HasDegreeLessThanOrEqualPrimitive
import org.neo4j.cypher.internal.physicalplanning.ast.HasDegreeLessThanPrimitive
import org.neo4j.cypher.internal.physicalplanning.ast.HasDegreePrimitive
import org.neo4j.cypher.internal.physicalplanning.ast.HasLabelsFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.HasTypesFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.IdFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.IsEmpty
import org.neo4j.cypher.internal.physicalplanning.ast.IsPrimitiveNull
import org.neo4j.cypher.internal.physicalplanning.ast.LabelsFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.NodeElementIdFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.NodeFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.NodeProperty
import org.neo4j.cypher.internal.physicalplanning.ast.NodePropertyExists
import org.neo4j.cypher.internal.physicalplanning.ast.NodePropertyExistsLate
import org.neo4j.cypher.internal.physicalplanning.ast.NodePropertyLate
import org.neo4j.cypher.internal.physicalplanning.ast.NonEmpty
import org.neo4j.cypher.internal.physicalplanning.ast.NullCheck
import org.neo4j.cypher.internal.physicalplanning.ast.NullCheckProperty
import org.neo4j.cypher.internal.physicalplanning.ast.NullCheckReferenceProperty
import org.neo4j.cypher.internal.physicalplanning.ast.NullCheckVariable
import org.neo4j.cypher.internal.physicalplanning.ast.PrimitiveAnds
import org.neo4j.cypher.internal.physicalplanning.ast.PrimitiveEquals
import org.neo4j.cypher.internal.physicalplanning.ast.PrimitiveNotEquals
import org.neo4j.cypher.internal.physicalplanning.ast.ReferenceFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.RelationshipElementIdFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.RelationshipFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.RelationshipProperty
import org.neo4j.cypher.internal.physicalplanning.ast.RelationshipPropertyExists
import org.neo4j.cypher.internal.physicalplanning.ast.RelationshipPropertyExistsLate
import org.neo4j.cypher.internal.physicalplanning.ast.RelationshipPropertyLate
import org.neo4j.cypher.internal.physicalplanning.ast.RelationshipTypeFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.SlottedCachedHasPropertyWithPropertyToken
import org.neo4j.cypher.internal.physicalplanning.ast.SlottedCachedHasPropertyWithoutPropertyToken
import org.neo4j.cypher.internal.physicalplanning.ast.SlottedCachedPropertyWithPropertyToken
import org.neo4j.cypher.internal.physicalplanning.ast.SlottedCachedPropertyWithoutPropertyToken
import org.neo4j.cypher.internal.physicalplanning.ast.prettifier.RuntimeExpressionStringifierTest.tokenContext
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.ast.DefaultValueLiteral
import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.cypher.internal.runtime.ast.MakeTraversable
import org.neo4j.cypher.internal.runtime.ast.ParameterFromSlot
import org.neo4j.cypher.internal.runtime.ast.PropertiesUsingCachedProperties
import org.neo4j.cypher.internal.runtime.ast.RuntimeConstant
import org.neo4j.cypher.internal.runtime.ast.RuntimeExpression
import org.neo4j.cypher.internal.runtime.ast.RuntimeProperty
import org.neo4j.cypher.internal.runtime.ast.RuntimeVariable
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.Endpoint.To
import org.neo4j.cypher.internal.runtime.ast.VariableRef
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values
import org.reflections.Reflections
import org.scalatest.prop.TableDrivenPropertyChecks

import java.lang.reflect.Modifier

import scala.jdk.CollectionConverters.CollectionHasAsScala

class RuntimeExpressionStringifierTest extends CypherFunSuite with AstConstructionTestSupport
    with TableDrivenPropertyChecks {

  private val slots: SlotConfiguration = SlotConfigurationBuilder.empty
    .newLong("x", nullable = true, CTNode)
    .newLong("r", nullable = true, CTRelationship)
    .newLong("a", nullable = true, typ = CTNode)
    .addAlias("alias", "a")
    .addAlias("rAlias", "r")
    .newArgument(Id(1))
    .newLong("  x@10", nullable = true, typ = CTNode)
    .newReference("y", nullable = true, CTNode)
    .newLong("  r@11", nullable = false, typ = CTRelationship)
    .build()

  private val readTokenContext: ReadTokenContext =
    tokenContext(nodeLabelTokens = Map(0 -> "A", 1 -> "B"), relationshipTypeTokens = Map(0 -> "R", 1 -> "S"))

  private val stringifier = RuntimeExpressionStringifier(readTokenContext, slots)

  private val ctx = ExpressionStringifier.apply(
    extensionStringifier = stringifier,
    alwaysParens = false,
    alwaysBacktick = false,
    preferSingleQuotes = false,
    sensitiveParamsAsParams = false,
    javaCompatible = false
  )

  private val supportedRuntimeExpressions = Table(
    ("runtime expression", "stringified"),
    (ParameterFromSlot(1, "p", IntegerType(isNullable = false)(InputPosition.NONE)), "$p"),
    (
      SlottedCachedPropertyWithPropertyToken.create(
        "x",
        propName("prop"),
        1,
        offsetIsForLongSlot = true,
        2,
        3,
        NODE_TYPE,
        nullable = false,
        needsValue = true,
        failOnMissingEntity = true
      ).asInstanceOf[RuntimeExpression],
      "x.prop"
    ),
    (
      SlottedCachedPropertyWithPropertyToken.create(
        "a",
        propName("prop"),
        1,
        offsetIsForLongSlot = true,
        2,
        3,
        NODE_TYPE,
        nullable = false,
        needsValue = true,
        failOnMissingEntity = true
      ).asInstanceOf[RuntimeExpression],
      "a.prop"
    ),
    (
      SlottedCachedPropertyWithoutPropertyToken("x", propName("prop"), 1, true, "prop", 1, NODE_TYPE, false, true),
      "x.prop"
    ),
    (
      SlottedCachedPropertyWithoutPropertyToken("a", propName("prop"), 1, true, "prop", 1, NODE_TYPE, false, true),
      "a.prop"
    ),
    (SlottedCachedHasPropertyWithPropertyToken("x", propName("prop"), 0, true, 1, 2, NODE_TYPE, false), "x.prop"),
    (
      SlottedCachedHasPropertyWithoutPropertyToken("x", propName("prop"), 0, true, "prop", 1, NODE_TYPE, false),
      "x.prop"
    ),
    (
      RuntimeConstant(varFor("x"), function("datetime", parameter("param", CTDate))),
      "datetime($param)"
    ),
    (HasAnyLabelFromSlot(0, Seq(0), Seq.empty), "x:A"),
    (HasAnyLabelFromSlot(0, Seq.empty, Seq("C")), "x:C"),
    (HasAnyLabelFromSlot(0, Seq(0), Seq("C")), "(x:A OR x:C)"),
    (HasAnyLabelFromSlot(0, Seq(0, 1), Seq("C", "D")), "(x:A OR x:B OR x:C OR x:D)"),
    (HasLabelsFromSlot(0, Seq(0), Seq.empty), "x:A"),
    (HasLabelsFromSlot(0, Seq.empty, Seq("C")), "x:C"),
    (HasLabelsFromSlot(0, Seq(0), Seq("C")), "(x:A AND x:C)"),
    (HasLabelsFromSlot(0, Seq(0, 1), Seq("C", "D")), "(x:A AND x:B AND x:C AND x:D)"),
    (HasTypesFromSlot(1, Seq(0), Seq()), "r:R"),
    (HasTypesFromSlot(1, Seq.empty, Seq("T")), "r:T"),
    (HasTypesFromSlot(1, Seq(0, 1), Seq("T", "U")), "(r:R AND r:S AND r:T AND r:U)"),
    (LabelsFromSlot(0), "labels(x)"),
    (RelationshipTypeFromSlot(1), "type(r)"),
    (PrimitiveEquals(0, 1), "x = r"),
    (PrimitiveNotEquals(0, 1), "NOT x = r"),
    (PrimitiveAnds(Seq(PrimitiveEquals(0, 1), PrimitiveNotEquals(1, 2))), "x = r AND NOT r = a"),
    (IsPrimitiveNull(0), "x IS NULL"),
    (NullCheck(0, varFor("x")), "x"),
    (NullCheck(0, varFor("z")), "z"),
    (NodeElementIdFromSlot(0), "elementId(x)"),
    (RelationshipElementIdFromSlot(1), "elementId(r)"),
    (HasALabelFromSlot(0), "x:%")
  )

  private val unsupportedExpressions = Table(
    "expressions",
    PropertiesUsingCachedProperties(varFor("x"), Set(cachedNodeProp("x", "prop"))),
    HasDegreeLessThanPrimitive(0, None, OUTGOING, literalInt(1)),
    HasDegreeLessThanOrEqualPrimitive(0, None, OUTGOING, literalInt(1)),
    HasDegreePrimitive(0, None, OUTGOING, literalInt(1)),
    HasDegreeGreaterThanOrEqualPrimitive(0, None, OUTGOING, literalInt(1)),
    HasDegreeGreaterThanPrimitive(1, None, INCOMING, literalInt(1)),
    IdFromSlot(1),
    DefaultValueLiteral(Values.intValue(1)),
    GetDegreePrimitive(1, None, BOTH),
    MakeTraversable(varFor("x")),
    TraversalEndpoint(varFor("x"), To),
    NonEmpty,
    IsEmpty
  )

  private val supportedRuntimeProperties = Table(
    ("runtime property", "stringified"),
    (NodeProperty(0, 1, "x.prop")(prop("x", "prop", InputPosition.NONE)), "x.prop"),
    (NodePropertyLate(0, "prop", "x.prop")(prop("x", "prop", InputPosition.NONE)), "x.prop"),
    (NodePropertyExists(0, 1, "x.prop")(prop("x", "prop", InputPosition.NONE)), "x.prop IS NOT NULL"),
    (NodePropertyExistsLate(0, "prop", "x.prop")(prop("x", "prop", InputPosition.NONE)), "x.prop IS NOT NULL"),
    (RelationshipProperty(0, 1, "x.prop")(prop("x", "prop", InputPosition.NONE)), "x.prop"),
    (RelationshipPropertyLate(0, "prop", "x.prop")(prop("x", "prop", InputPosition.NONE)), "x.prop"),
    (RelationshipPropertyExists(0, 1, "x.prop")(prop("x", "prop", InputPosition.NONE)), "x.prop IS NOT NULL"),
    (RelationshipPropertyExistsLate(0, "prop", "x.prop")(prop("x", "prop", InputPosition.NONE)), "x.prop IS NOT NULL"),
    (NullCheckProperty(0, prop("x", "prop", InputPosition.NONE)), "x.prop"),
    (NullCheckProperty(0, NodePropertyLate(0, "prop", "x.prop")(prop("x", "prop", InputPosition.NONE))), "x.prop"),
    (
      NullCheckReferenceProperty(
        0,
        SlottedCachedPropertyWithPropertyToken(
          "y",
          propName("prop"),
          0,
          offsetIsForLongSlot = false,
          2,
          3,
          NODE_TYPE,
          nullable = false,
          failOnMissingEntity = true
        )
      ),
      "y.prop"
    )
  )

  private val unsupportedRuntimeProperties = Table[RuntimeProperty](
    "runtime property"
  )

  private val supportedRuntimeVariables = Table(
    ("runtime variable", "stringified"),
    (NodeFromSlot(0, "x"), "x"),
    (RelationshipFromSlot(1, "r"), "r"),
    (NullCheckVariable(0, varFor("x")), "x"),
    (ReferenceFromSlot(0, "y"), "y"),
    (VariableRef("x"), "x"),
    (ExpressionVariable(0, "x"), "x")
  )

  private val unsupportedRuntimeVariables = Table[RuntimeVariable](
    "runtime variable"
  )

  test("should stringify runtime expression") {
    forEvery(supportedRuntimeExpressions) { (expression, stringified) =>
      stringifier(ctx)(expression) shouldBe stringified
    }
  }

  test("should throw for unsupported expressions") {
    forEvery(unsupportedExpressions) { expression =>
      assertThrows[UnsupportedOperationException](stringifier(ctx)(expression))
    }
  }

  test("should backtick generated names") {
    val generatedVariables = Table(
      ("expression", "stringified"),
      (
        SlottedCachedPropertyWithPropertyToken(
          "  x@10",
          propName("prop"),
          4,
          true,
          1,
          2,
          NODE_TYPE,
          nullable = false,
          failOnMissingEntity = true
        ),
        "`  x@10`.prop"
      ),
      (NodeProperty(4, 1, "  x@10.prop")(prop("  x@10", "prop", InputPosition.NONE)), "`  x@10`.prop"),
      (
        NodePropertyExists(99, 1, "  x@10.prop")(prop("  x@10", "prop", InputPosition.NONE)),
        "`  x@10`.prop IS NOT NULL"
      ),
      (NodeElementIdFromSlot(4), "elementId(`  x@10`)"),
      (RelationshipElementIdFromSlot(5), "elementId(`  r@11`)")
    )

    forEvery(generatedVariables) { (expression, stringified) =>
      stringifier(ctx)(expression) shouldBe stringified
    }
  }

  test("should backtick generated parameter names") {
    stringifier(ctx)(ParameterFromSlot(0, " p0", IntegerType(isNullable = false)(InputPosition.NONE))) shouldBe "$` p0`"
  }

  test("should resolve alias to original slot variable name") {

    val aliasedVariables = Table(
      ("expression", "stringified"),
      (
        SlottedCachedPropertyWithPropertyToken(
          "alias",
          propName("prop"),
          1,
          offsetIsForLongSlot = true,
          2,
          3,
          NODE_TYPE,
          nullable = false,
          failOnMissingEntity = false
        ).asInstanceOf[RuntimeExpression],
        "a.prop"
      ),
      (
        SlottedCachedPropertyWithoutPropertyToken(
          "alias",
          propName("prop"),
          1,
          true,
          "prop",
          1,
          NODE_TYPE,
          false,
          false
        ),
        "a.prop"
      ),
      (NodeFromSlot(2, "alias"), "a"),
      (RelationshipFromSlot(1, "rAlias"), "r"),
      (NodeProperty(2, 1, "alias.prop")(prop("alias", "prop", InputPosition.NONE)), "a.prop"),
      (NodePropertyExists(2, 1, "alias.prop")(prop("alias", "prop", InputPosition.NONE)), "a.prop IS NOT NULL"),
      (RelationshipProperty(1, 1, "rAlias.prop")(prop("rAlias", "prop", InputPosition.NONE)), "r.prop")
    )
    forEvery(aliasedVariables) { (expression, stringified) =>
      stringifier(ctx)(expression) shouldBe stringified
    }
  }

  test("should test all runtime expressions") {
    val reflections = new Reflections("org.neo4j.cypher")

    val allRuntimeExpressions: Set[Class[_ <: RuntimeExpression]] =
      reflections.getSubTypesOf[RuntimeExpression](classOf[RuntimeExpression]).asScala
        .filter(planClass => !Modifier.isAbstract(planClass.getModifiers)).toSet

    val testedClasses: Set[Class[_ <: RuntimeExpression]] =
      supportedRuntimeExpressions.map(_._1.getClass).toSet ++ unsupportedExpressions.map(_.getClass).toSet

    withClue("tests missing for these runtime expressions: ") {
      allRuntimeExpressions should not be empty
      allRuntimeExpressions -- testedClasses should be(empty)
    }
  }

  test("should stringify runtime properties") {
    forEvery(supportedRuntimeProperties) { (expression, stringified) =>
      stringifier(ctx)(expression) shouldBe stringified
    }
  }

  test("should throw for unsupported runtime properties") {
    forEvery(unsupportedRuntimeProperties) { expression =>
      assertThrows[UnsupportedOperationException](stringifier(ctx)(expression))
    }
  }

  test("should throw for long slot offset out of bounds") {
    val oob = slots.numberOfLongs
    val offsetOOB = HasAnyLabelFromSlot(offset = oob, Seq(0), Seq.empty)
    val ex = intercept[IllegalArgumentException](stringifier(ctx)(offsetOOB))
    ex.getMessage shouldEqual s"No LongSlot with offset $oob."
  }

  test("should throw for ref slot offset out of bounds") {
    val oob = slots.numberOfReferences
    val offsetOOB = ReferenceFromSlot(offset = oob, "y")
    val ex = intercept[IllegalArgumentException](stringifier(ctx)(offsetOOB))
    ex.getMessage shouldEqual s"No RefSlot with offset $oob."
  }

  test("should throw for unexpected slot key type") {
    val notVariableSlotOffset = HasAnyLabelFromSlot(3, Seq(0), Seq.empty)
    val ex = intercept[IllegalArgumentException](stringifier(ctx)(notVariableSlotOffset))
    ex.getMessage shouldEqual s"No LongSlot with offset 3."

  }

  test("should test all RuntimeProperties") {
    val reflections = new Reflections("org.neo4j.cypher")

    val allRuntimeProperties: Set[Class[_ <: RuntimeProperty]] =
      reflections.getSubTypesOf[RuntimeProperty](classOf[RuntimeProperty]).asScala
        .filter(planClass => !Modifier.isAbstract(planClass.getModifiers)).toSet

    val testedClasses: Set[Class[_ <: RuntimeProperty]] =
      supportedRuntimeProperties.map(_._1.getClass).toSet ++ unsupportedRuntimeProperties.map(_.getClass).toSet

    withClue("tests missing for these runtime expressions: ") {
      allRuntimeProperties should not be empty
      allRuntimeProperties -- testedClasses should be(empty)
    }
  }

  test("should stringify runtime variables") {
    forEvery(supportedRuntimeVariables) { (expression, stringified) =>
      stringifier(ctx)(expression) shouldBe stringified
    }
  }

  test("should throw for unsupported runtime variables") {
    forEvery(unsupportedRuntimeVariables) { expression =>
      assertThrows[UnsupportedOperationException](stringifier(ctx)(expression))
    }
  }

  test("should test all RuntimeVariables") {
    val reflections = new Reflections("org.neo4j.cypher")

    val allRuntimeProperties: Set[Class[_ <: RuntimeVariable]] =
      reflections.getSubTypesOf[RuntimeVariable](classOf[RuntimeVariable]).asScala
        .filter(planClass => !Modifier.isAbstract(planClass.getModifiers)).toSet

    val testedClasses: Set[Class[_ <: RuntimeVariable]] =
      supportedRuntimeVariables.map(_._1.getClass).toSet ++ unsupportedRuntimeVariables.map(_.getClass).toSet

    withClue("tests missing for these runtime expressions: ") {
      allRuntimeProperties should not be empty
      allRuntimeProperties -- testedClasses should be(empty)
    }
  }

  test("should escape property keys") {
    val property = prop("x", "property ` 1", InputPosition.NONE)
    val expressions = Table(
      ("expression", "expected"),
      (NodeProperty(0, 0, "property ` 1")(property), "x.`property `` 1`"),
      (NodePropertyLate(0, "property ` 1", "x.`property `` 1`")(property), "x.`property `` 1`"),
      (NodePropertyExists(0, 1, "x.`property `` 1`")(property), "x.`property `` 1` IS NOT NULL"),
      (NodePropertyExistsLate(0, "property ` 1", "x.`property `` 1`")(property), "x.`property `` 1` IS NOT NULL"),
      (RelationshipProperty(0, 1, "x.`property `` 1`")(property), "x.`property `` 1`"),
      (RelationshipPropertyLate(0, "property ` 1", "x.`property `` 1`")(property), "x.`property `` 1`"),
      (RelationshipPropertyExists(0, 1, "x.`property `` 1`")(property), "x.`property `` 1` IS NOT NULL"),
      (
        RelationshipPropertyExistsLate(0, "property ` 1", "x.`property `` 1`")(property),
        "x.`property `` 1` IS NOT NULL"
      ),
      (
        SlottedCachedPropertyWithPropertyToken(
          "x",
          propName("property ` 1"),
          1,
          true,
          2,
          3,
          NODE_TYPE,
          false,
          true
        ),
        "x.`property `` 1`"
      ),
      (
        SlottedCachedPropertyWithoutPropertyToken(
          "x",
          propName("property ` 1"),
          1,
          true,
          "property ` 1",
          1,
          NODE_TYPE,
          false,
          false
        ),
        "x.`property `` 1`"
      ),
      (
        SlottedCachedHasPropertyWithPropertyToken("x", propName("property ` 1"), 0, true, 1, 2, NODE_TYPE, false),
        "x.`property `` 1`"
      ),
      (
        SlottedCachedHasPropertyWithoutPropertyToken(
          "x",
          propName("property ` 1"),
          0,
          true,
          "property ` 1",
          1,
          NODE_TYPE,
          false
        ),
        "x.`property `` 1`"
      )
    )

    forEvery(expressions) { (expression, stringified) =>
      stringifier(ctx)(expression) shouldBe stringified
    }
  }
}

object RuntimeExpressionStringifierTest {

  private def tokenContext(
    nodeLabelTokens: Map[Int, String],
    relationshipTypeTokens: Map[Int, String]
  ): ReadTokenContext =
    new ReadTokenContext {
      override def getLabelName(id: Int): String = nodeLabelTokens(id)

      override def getOptLabelId(labelName: String): Option[Int] = ???

      override def getLabelId(labelName: String): Int = ???

      override def getPropertyKeyName(id: Int): String = ???

      override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = ???

      override def getPropertyKeyId(propertyKeyName: String): Int = ???

      override def getRelTypeName(id: Int): String = relationshipTypeTokens(id)

      override def getOptRelTypeId(relType: String): Option[Int] = ???

      override def getRelTypeId(relType: String): Int = ???
    }
}
