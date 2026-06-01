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
package org.neo4j.cypher.internal.ast.generator

import org.neo4j.cypher.internal.ast.generator.RememberToUpdateAstGeneratorTest.astNodeSubTypes
import org.neo4j.cypher.internal.ast.generator.RememberToUpdateAstGeneratorTest.ignoreAstClasses
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.reflections.Reflections

import java.lang.reflect.Modifier

import scala.io.Source
import scala.jdk.CollectionConverters.SetHasAsScala
import scala.util.Using

class RememberToUpdateAstGeneratorTest extends CypherFunSuite {

  test("remember to update AstGenerator") {
    val astGeneratorSourcePath = "sources/org/neo4j/cypher/internal/ast/generator/AstGenerator.scala"
    val importPattern = """^import (.*)$""".r

    val imports = Using.resource(Source.fromResource(astGeneratorSourcePath)) { astGenSource =>
      astGenSource.getLines()
        .collect { case importPattern(importClass) => importClass }
        .toSet
    }
    val astClasses = astNodeSubTypes().toSet
    astClasses.size should be > 300
    val missing = astClasses.diff(imports).diff(ignoreAstClasses)
    if (missing.nonEmpty) {
      fail(
        s"""Hi, it looks like you have introduced a new class inheriting ${classOf[ASTNode].getName}.
           |New classes:
           |${missing.mkString("\n")}
           |
           |Please take one (or both) of the the following actions.
           |
           |- Recommended, make sure ${classOf[AstGenerator].getName} can generate the new AST class, if it imports the new class this test should be green.
           |- Add the new classes to RememberToUpdateAstGeneratorTest.ignoreAstClasses to make this test pass without adding it as an import in ${classOf[
            AstGenerator
          ].getSimpleName}.
           |
           |Sorry for the inconvenience and good luck with the new AST.
           |""".stripMargin
      )
    }
  }
}

object RememberToUpdateAstGeneratorTest {
  val namespace = "org.neo4j.cypher.internal"

  def astNodeSubTypes(): Seq[String] = {
    new Reflections(namespace).getSubTypesOf[ASTNode](classOf[ASTNode]).asScala.iterator
      .filter(c => !c.isInterface && !c.getName.contains("$$anon$") && !Modifier.isAbstract(c.getModifiers))
      .map(c => c.getName.replace("$", "."))
      .toSeq
      .sorted
  }

  val ignoreAstClasses: Set[String] = Set(
    "org.neo4j.cypher.internal.ast.CreateConstraintCommand",
    "org.neo4j.cypher.internal.ast.CreateFulltextIndexCommand",
    "org.neo4j.cypher.internal.ast.CreateLookupIndexCommand",
    "org.neo4j.cypher.internal.ast.CreatePropertyTypeConstraint",
    "org.neo4j.cypher.internal.ast.CreateSingleLabelPropertyIndexCommand",
    "org.neo4j.cypher.internal.ast.CreateVectorIndexCommand",
    "org.neo4j.cypher.internal.ast.ExternalAuth",
    "org.neo4j.cypher.internal.ast.InputDataStream",
    "org.neo4j.cypher.internal.ast.NativeAuth",
    "org.neo4j.cypher.internal.ast.NoNames.", // This is a case object, and in this class that seems to need a trailing `.`
    "org.neo4j.cypher.internal.ast.NoOptions.", // This is a case object, and in this class that seems to need a trailing `.`
    "org.neo4j.cypher.internal.ast.ProjectingUnionAll",
    "org.neo4j.cypher.internal.ast.ProjectingUnionDistinct",
    "org.neo4j.cypher.internal.ast.RemoveDynamicPropertyItem",
    "org.neo4j.cypher.internal.ast.semantics.SemanticPatternCheck.ScopeAfterParenthesizedPath",
    "org.neo4j.cypher.internal.ast.semantics.SemanticPatternCheck.ScopeBeforeParenthesizedPath",
    "org.neo4j.cypher.internal.ast.SetDynamicPropertyItem",
    "org.neo4j.cypher.internal.ast.SetPropertyItems",
    "org.neo4j.cypher.internal.ast.Statements",
    "org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsRetryParameters", // Temporary, until parser changes are merged
    "org.neo4j.cypher.internal.ast.UsingExpandStepHint",
    "org.neo4j.cypher.internal.ast.UsingStatefulShortestPathAll",
    "org.neo4j.cypher.internal.ast.UsingStatefulShortestPathInto",
    "org.neo4j.cypher.internal.ast.VectorValueConstructor",
    "org.neo4j.cypher.internal.expressions.AllReduceAccumulator",
    "org.neo4j.cypher.internal.expressions.AllReducePredicate.AllReduceScope",
    "org.neo4j.cypher.internal.expressions.AllReducePredicate.ReductionStepVariableScope",
    "org.neo4j.cypher.internal.expressions.AllReduceSingletonPredicate",
    "org.neo4j.cypher.internal.expressions.AndedPropertyInequalities",
    "org.neo4j.cypher.internal.expressions.AndsReorderable",
    "org.neo4j.cypher.internal.expressions.AssertIsNode",
    "org.neo4j.cypher.internal.expressions.AutoExtractedParameter",
    "org.neo4j.cypher.internal.expressions.CachedHasProperty",
    "org.neo4j.cypher.internal.expressions.CachedProperty",
    "org.neo4j.cypher.internal.expressions.CoerceTo",
    "org.neo4j.cypher.internal.expressions.CollectAll",
    "org.neo4j.cypher.internal.expressions.CollectDistinct",
    "org.neo4j.cypher.internal.expressions.CollectDistinctIds",
    "org.neo4j.cypher.internal.expressions.Concatenate",
    "org.neo4j.cypher.internal.expressions.DesugaredMapProjection",
    "org.neo4j.cypher.internal.expressions.DifferentNodes",
    "org.neo4j.cypher.internal.expressions.DifferentRelationships",
    "org.neo4j.cypher.internal.expressions.Disjoint",
    "org.neo4j.cypher.internal.expressions.DisjointNodes",
    "org.neo4j.cypher.internal.expressions.ElementIdToLongId",
    "org.neo4j.cypher.internal.expressions.GetDegree",
    "org.neo4j.cypher.internal.expressions.HasDegree",
    "org.neo4j.cypher.internal.expressions.HasDegreeGreaterThan",
    "org.neo4j.cypher.internal.expressions.HasDegreeGreaterThanOrEqual",
    "org.neo4j.cypher.internal.expressions.HasDegreeLessThan",
    "org.neo4j.cypher.internal.expressions.HasDegreeLessThanOrEqual",
    "org.neo4j.cypher.internal.expressions.ImplicitProcedureArgument",
    "org.neo4j.cypher.internal.expressions.IsRepeatTrailUnique",
    "org.neo4j.cypher.internal.expressions.IsRepeatAcyclic",
    "org.neo4j.cypher.internal.expressions.MatchMode.DifferentRelationships",
    "org.neo4j.cypher.internal.expressions.MatchMode.RepeatableElements",
    "org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep",
    "org.neo4j.cypher.internal.expressions.NilPathStep",
    "org.neo4j.cypher.internal.expressions.NodePathStep",
    "org.neo4j.cypher.internal.expressions.NoneOfNodesInVarLengthRelationship",
    "org.neo4j.cypher.internal.expressions.NonCompilable",
    "org.neo4j.cypher.internal.expressions.NoneOfRelationships",
    "org.neo4j.cypher.internal.expressions.NoneOfNodes",
    "org.neo4j.cypher.internal.expressions.NullCheckAssert",
    "org.neo4j.cypher.internal.expressions.Ors",
    "org.neo4j.cypher.internal.expressions.ParenthesizedPath",
    "org.neo4j.cypher.internal.expressions.PartialPredicate.PartialDistanceSeekWrapper",
    "org.neo4j.cypher.internal.expressions.PartialPredicate.PartialPredicateWrapper",
    "org.neo4j.cypher.internal.expressions.PathExpression",
    "org.neo4j.cypher.internal.expressions.Pattern.ForMatch",
    "org.neo4j.cypher.internal.expressions.Pattern.ForUpdate",
    "org.neo4j.cypher.internal.expressions.SingleRelationshipPathStep",
    "org.neo4j.cypher.internal.expressions.Unique",
    "org.neo4j.cypher.internal.expressions.UniqueNodes",
    "org.neo4j.cypher.internal.expressions.VariableGrouping",
    "org.neo4j.cypher.internal.expressions.VarLengthLowerBound",
    "org.neo4j.cypher.internal.expressions.VarLengthUpperBound",
    "org.neo4j.cypher.internal.expressions.VectorSearchPredicate",
    "org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonConjunction",
    "org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonDisjunction",
    "org.neo4j.cypher.internal.label_expressions.LabelExpressionPredicate",
    "org.neo4j.cypher.internal.logical.plans.CoerceToPredicate",
    "org.neo4j.cypher.internal.util.symbols.BooleanType",
    "org.neo4j.cypher.internal.util.symbols.DateType",
    "org.neo4j.cypher.internal.util.symbols.DurationType",
    "org.neo4j.cypher.internal.util.symbols.FloatType",
    "org.neo4j.cypher.internal.util.symbols.Float32Type",
    "org.neo4j.cypher.internal.util.symbols.GeometryType",
    "org.neo4j.cypher.internal.util.symbols.GraphRefType",
    "org.neo4j.cypher.internal.util.symbols.IntegerType",
    "org.neo4j.cypher.internal.util.symbols.Integer32Type",
    "org.neo4j.cypher.internal.util.symbols.Integer16Type",
    "org.neo4j.cypher.internal.util.symbols.Integer8Type",
    "org.neo4j.cypher.internal.util.symbols.LocalDateTimeType",
    "org.neo4j.cypher.internal.util.symbols.LocalTimeType",
    "org.neo4j.cypher.internal.util.symbols.MapType",
    "org.neo4j.cypher.internal.util.symbols.NodeType",
    "org.neo4j.cypher.internal.util.symbols.NothingType",
    "org.neo4j.cypher.internal.util.symbols.NullType",
    "org.neo4j.cypher.internal.util.symbols.NumberType",
    "org.neo4j.cypher.internal.util.symbols.PathType",
    "org.neo4j.cypher.internal.util.symbols.PointType",
    "org.neo4j.cypher.internal.util.symbols.PropertyValueType",
    "org.neo4j.cypher.internal.util.symbols.RelationshipType",
    "org.neo4j.cypher.internal.util.symbols.StringType",
    "org.neo4j.cypher.internal.util.symbols.VectorType",
    "org.neo4j.cypher.internal.util.symbols.ZonedDateTimeType",
    "org.neo4j.cypher.internal.util.symbols.ZonedTimeType",
    "org.neo4j.cypher.internal.expressions.RepeatPathStep",
    "org.neo4j.cypher.internal.ast.semantics.MapExtendedType",
    "org.neo4j.cypher.internal.ast.semantics.scoping.DummyASTNode.",
    "org.neo4j.cypher.internal.expressions.RepeatPathStep",
    "org.neo4j.cypher.internal.expressions.ImpliedLabel",
    "org.neo4j.cypher.internal.expressions.DummyExpression",
    "org.neo4j.cypher.internal.expressions.ObfuscatedLiteral"
  )
}
