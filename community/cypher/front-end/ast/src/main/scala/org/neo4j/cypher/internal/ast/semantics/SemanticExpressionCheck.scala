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
package org.neo4j.cypher.internal.ast.semantics

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.CypherTypeName
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.ImportingWithSubqueryCall
import org.neo4j.cypher.internal.ast.IsNormalized
import org.neo4j.cypher.internal.ast.IsNotNormalized
import org.neo4j.cypher.internal.ast.IsNotTyped
import org.neo4j.cypher.internal.ast.IsTyped
import org.neo4j.cypher.internal.ast.ScopeClauseSubqueryCall
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.VectorValueConstructor
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.fromContext
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.fromState
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.success
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.when
import org.neo4j.cypher.internal.ast.semantics.SemanticPatternCheck.TokenType
import org.neo4j.cypher.internal.ast.semantics.SemanticPatternCheck.checkValidLabels
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.AllPropertiesSelector
import org.neo4j.cypher.internal.expressions.AllReducePredicate
import org.neo4j.cypher.internal.expressions.AllReducePredicate.AccumulatorReductionTypeMismatchMessageGenerator
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.BooleanLiteral
import org.neo4j.cypher.internal.expressions.CachedHasProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.CoerceTo
import org.neo4j.cypher.internal.expressions.Concatenate
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.DesugaredMapProjection
import org.neo4j.cypher.internal.expressions.DifferentNodes
import org.neo4j.cypher.internal.expressions.DifferentRelationships
import org.neo4j.cypher.internal.expressions.Disjoint
import org.neo4j.cypher.internal.expressions.DisjointNodes
import org.neo4j.cypher.internal.expressions.Divide
import org.neo4j.cypher.internal.expressions.DoubleLiteral
import org.neo4j.cypher.internal.expressions.DynamicLabelsExpressions
import org.neo4j.cypher.internal.expressions.DynamicLabelsOrTypeExpressions
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Expression.DefaultTypeMismatchMessageGenerator
import org.neo4j.cypher.internal.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.expressions.ExtractScope
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.FilteringExpression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.GetDegree
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasAnyDynamicType
import org.neo4j.cypher.internal.expressions.HasDynamicType
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.HexIntegerLiteral
import org.neo4j.cypher.internal.expressions.ImplicitProcedureArgument
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.Infinity
import org.neo4j.cypher.internal.expressions.IntegerLiteral
import org.neo4j.cypher.internal.expressions.InvalidNotEquals
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.IterablePredicateExpression
import org.neo4j.cypher.internal.expressions.LabelCheckExpression
import org.neo4j.cypher.internal.expressions.LabelOrTypeCheckExpression
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.ListSlice
import org.neo4j.cypher.internal.expressions.LiteralEntry
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.MapProjection
import org.neo4j.cypher.internal.expressions.Modulo
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.Multiply
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.NaN
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NoneOfNodes
import org.neo4j.cypher.internal.expressions.NoneOfRelationships
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.NumberLiteral
import org.neo4j.cypher.internal.expressions.ObfuscatedLiteral
import org.neo4j.cypher.internal.expressions.OctalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.Pow
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyExists
import org.neo4j.cypher.internal.expressions.PropertySelector
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.ReduceExpression
import org.neo4j.cypher.internal.expressions.ReduceExpression.AccumulatorExpressionTypeMismatchMessageGenerator
import org.neo4j.cypher.internal.expressions.ReduceScope
import org.neo4j.cypher.internal.expressions.RegexMatch
import org.neo4j.cypher.internal.expressions.RepeatPathStep
import org.neo4j.cypher.internal.expressions.ShortestPathExpression
import org.neo4j.cypher.internal.expressions.SingleRelationshipPathStep
import org.neo4j.cypher.internal.expressions.StartsWith
import org.neo4j.cypher.internal.expressions.StringDecimalInteger
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Subtract
import org.neo4j.cypher.internal.expressions.UnaryAdd
import org.neo4j.cypher.internal.expressions.UnarySubtract
import org.neo4j.cypher.internal.expressions.Unique
import org.neo4j.cypher.internal.expressions.UniqueNodes
import org.neo4j.cypher.internal.expressions.VarLengthBound
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.VariableSelector
import org.neo4j.cypher.internal.expressions.Xor
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonDisjunction
import org.neo4j.cypher.internal.label_expressions.LabelExpressionPredicate
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTLocalDateTime
import org.neo4j.cypher.internal.util.symbols.CTLocalTime
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTTime
import org.neo4j.cypher.internal.util.symbols.CTVector
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.StorableType.storableType
import org.neo4j.cypher.internal.util.symbols.TypeSpec
import org.neo4j.cypher.internal.util.symbols.TypeSpecRange
import org.neo4j.cypher.internal.util.symbols.invariantTypeSpec
import org.neo4j.values.storable.VectorValue

object SemanticExpressionCheck extends SemanticAnalysisTooling {

  val crashOnUnknownExpression: (SemanticContext, Expression) => SemanticCheck =
    (_, e) => throw new UnsupportedOperationException(s"Error in semantic analysis: Unknown expression $e")

  /**
   * This fallback allow for a testing backdoor to insert custom Expressions. Do not use in production.
   */
  var semanticCheckFallback: (SemanticContext, Expression) => SemanticCheck = crashOnUnknownExpression

  /**
   * Build a semantic check for the given expression using the simple expression context.
   * The simple expression context disallows aggregating functions.
   */
  def simple(expression: Expression): SemanticCheck = check(SemanticContext.Simple, expression)

  object TypeMismatchContext extends Enumeration {

    case class TypeMismatchContextVal(txt: String) {
      def getText: String = txt
    }
    val ACCUMULATOR: TypeMismatchContextVal = TypeMismatchContextVal("accumulator")
    val LIST_INDEX: TypeMismatchContextVal = TypeMismatchContextVal("list index")
    val MAP_KEY: TypeMismatchContextVal = TypeMismatchContextVal("map key")
    val DYNAMIC_LABEL: TypeMismatchContextVal = TypeMismatchContextVal("dynamic label")
    val DYNAMIC_TYPE: TypeMismatchContextVal = TypeMismatchContextVal("dynamic type")

    val NODE_OR_RELATIONSHIP_PROPERTY_KEY: TypeMismatchContextVal =
      TypeMismatchContextVal("node or relationship property key")
    val EMPTY: TypeMismatchContextVal = TypeMismatchContextVal("")
    // Note: EMPTY will lead to
    //   GQL code 22NB1:  Type mismatch: expected to be one of { %s } but was { %s }.
    // All other TypeMismatchContextVal's will lead to
    //   GQL code 22N27: Invalid input { %s } for { %s }. Expected to be one of { %s }.
    // TypeMismatchContextVal.txt will be used here ^
  }

  /**
   * Build a semantic check for the given expression and context.
   */
  def check(
    ctx: SemanticContext,
    expression: Expression
  ): SemanticCheck = SemanticCheck.nestedCheck {
    expression match {

      // ARITHMETICS

      case x: Add =>
        check(ctx, x.lhs) chain
          expectType(TypeSpec.all, x.lhs) chain
          check(ctx, x.rhs) chain
          expectType(infixAddRhsTypes(x.lhs), x.rhs) chain
          specifyType(infixAddOutputTypes(x.lhs, x.rhs), x)

      case x: Concatenate =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures) chain
          specifyType(infixAddOutputTypes(x.lhs, x.rhs), x)

      case x: Subtract =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x: UnarySubtract =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x: UnaryAdd =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x: Multiply =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x: Divide =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x: Modulo =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x: Pow =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      // PREDICATES

      case x: Not =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x: Equals =>
        check(ctx, x.arguments) chain checkTypes(x, x.signatures)

      case x: NotEquals =>
        check(ctx, x.arguments) chain checkTypes(x, x.signatures)

      case x: InvalidNotEquals => SemanticError.wrongInequalityOperator(x.position)

      case x: RegexMatch =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x: And =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x: Or =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x: Xor =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x: Ands =>
        check(ctx, x.exprs) chain
          checkTypes(x, x.signatures)

      case x: Ors =>
        check(ctx, x.exprs) chain
          checkTypes(x, x.signatures)

      case x: In =>
        check(ctx, x.lhs) chain
          expectType(CTAny.covariant, x.lhs) chain
          check(ctx, x.rhs) chain
          expectType(CTList(CTAny).covariant, x.rhs) chain
          specifyType(CTBoolean, x)

      case x: StartsWith =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x: EndsWith =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x: Contains =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x: IsNull =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x: IsNotNull =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures)

      case x: PropertyExists =>
        check(ctx, x.arguments) chain
          expectType(CTNode.covariant | CTRelationship.covariant, x.element) chain
          specifyType(CTBoolean, x)

      case x: IsTyped =>
        check(ctx, x.arguments) chain
          CypherTypeName(x.typeName).semanticCheck chain
          checkTypes(x, x.signatures) chain
          SemanticCheck.success

      case x: IsNotTyped =>
        check(ctx, x.arguments) chain
          CypherTypeName(x.typeName).semanticCheck chain
          checkTypes(x, x.signatures) chain
          SemanticCheck.success

      case x: IsNormalized =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures) chain
          specifyType(CTBoolean, x)

      case x: IsNotNormalized =>
        check(ctx, x.arguments) chain
          checkTypes(x, x.signatures) chain
          specifyType(CTBoolean, x)

      case x: LessThan =>
        check(ctx, x.arguments) chain checkTypes(x, x.signatures)

      case x: LessThanOrEqual =>
        check(ctx, x.arguments) chain checkTypes(x, x.signatures)

      case x: GreaterThan =>
        check(ctx, x.arguments) chain checkTypes(x, x.signatures)

      case x: GreaterThanOrEqual =>
        check(ctx, x.arguments) chain checkTypes(x, x.signatures)

      case x @ DifferentRelationships(lhs, rhs) =>
        check(ctx, x.arguments) chain
          expectType(CTRelationship, lhs) chain
          expectType(CTRelationship, rhs) chain
          specifyType(CTBoolean, x)

      case x @ DifferentNodes(lhs, rhs) =>
        check(ctx, x.arguments) chain
          expectType(CTNode, lhs) chain
          expectType(CTNode, rhs) chain
          specifyType(CTBoolean, x)

      case x @ NoneOfRelationships(relationship, relationshipList) =>
        check(ctx, x.arguments) chain
          expectType(CTRelationship, relationship) chain
          expectType(CTList(CTRelationship), relationshipList) chain
          specifyType(CTBoolean, x)

      case x @ NoneOfNodes(node, nodeList) =>
        check(ctx, x.arguments) chain
          expectType(CTNode, node) chain
          expectType(CTList(CTNode), nodeList) chain
          specifyType(CTBoolean, x)

      case x @ Disjoint(lhs, rhs) =>
        check(ctx, x.arguments) chain
          expectType(CTList(CTAny).covariant, lhs) chain
          expectType(CTList(CTAny).covariant, rhs) chain
          specifyType(CTBoolean, x)

      case x @ DisjointNodes(lhs, rhs, _, _) =>
        check(ctx, x.arguments) chain
          expectType(CTList(CTNode).covariant, lhs) chain
          expectType(CTList(CTNode).covariant, rhs) chain
          specifyType(CTBoolean, x)

      case x @ Unique(rhs) =>
        check(ctx, x.arguments) chain
          expectType(CTList(CTAny).covariant, rhs) chain
          specifyType(CTBoolean, x)

      case x @ UniqueNodes(nodeList, relList) =>
        check(ctx, x.arguments) chain
          expectType(CTList(CTNode).covariant, nodeList) chain
          expectType(CTList(CTRelationship).covariant, relList) chain
          specifyType(CTBoolean, x)

      case x: VarLengthBound =>
        check(ctx, x.arguments) chain
          expectType(CTList(CTRelationship).covariant, x.relName) chain
          specifyType(CTBoolean, x)

      case x: VectorValueConstructor =>
        check(ctx, x.arguments) chain
          CypherTypeName(x.typeName).semanticCheck chain
          expectType(
            CTInteger.covariant,
            x.dimension,
            TypeMismatchContext.TypeMismatchContextVal(
              s"argument at index 1 of function vector()"
            ),
            DefaultTypeMismatchMessageGenerator
          ) chain
          expectType(
            ClosedDynamicUnionType(Set(CTList(CTNumber), CTString))(InputPosition.NONE).covariant,
            x.vectorCandidate,
            TypeMismatchContext.TypeMismatchContextVal(
              s"argument at index 0 of function vector()"
            ),
            DefaultTypeMismatchMessageGenerator
          ) chain
          literalShouldBeNumberInRange(
            x.dimension,
            "dimension",
            VectorValue.MIN_VECTOR_DIMENSIONS,
            VectorValue.MAX_VECTOR_DIMENSIONS
          ) chain
          when(
            !x.validVectorInnerType
          ) {
            error(SemanticError.invalidType(
              x.typeName.toCypherTypeString,
              List(
                "INTEGER64",
                "INTEGER32",
                "INTEGER16",
                "INTEGER8",
                "FLOAT64",
                "FLOAT32"
              ),
              x.typeName.toCypherTypeString,
              "Invalid vector inner type, expected INTEGER64, INTEGER32, INTEGER16, INTEGER8, FLOAT64 or FLOAT32",
              x.dimension.position
            ))
          } chain
          specifyType(CTVector, x)

      case x: PartialPredicate[_] =>
        check(ctx, x.coveredPredicate)

      case x: CaseExpression =>
        val possibleTypes = unionOfTypes(x.possibleExpressions)
        SemanticExpressionCheck.check(ctx, x.expression) chain
          check(ctx, x.alternatives.flatMap { a => Seq(a._1, a._2) }) chain
          check(ctx, x.default) chain
          expectType(CTBoolean.covariant, x.alternatives.map(_._1)) chain
          specifyType(possibleTypes, x)

      case x: AndedPropertyInequalities =>
        x.inequalities.map(check(ctx, _)).reduceLeft(_ chain _)

      case x: CoerceTo =>
        check(ctx, x.expr) chain expectType(x.typ.covariant, x.expr)

      case x: Property =>
        val allowedTypes =
          CTNode.covariant | CTRelationship.covariant | CTMap.covariant | CTPoint.covariant | CTDate.covariant | CTTime.covariant |
            CTLocalTime.covariant | CTLocalDateTime.covariant | CTDateTime.covariant | CTDuration.covariant

        check(ctx, x.map) chain
          expectType(allowedTypes, x.map) chain
          typeSwitch(x.map) {
            // Maybe we can do even more here - Point / Dates probably have type implications too
            // `invariant` is a `def` on CypherType (TeaVM-friendly); not a stable pattern, so use a guard.
            case t if t == CTNode.invariant || t == CTRelationship.invariant => specifyType(storableType, x)
            case TypeSpecRange(_, extendedType: MapExtendedType) =>
              val entryType = extendedType.getEntryType(x.propertyKey.name)
              specifyType(entryType, x)
            case _ => specifyType(CTAny.covariant, x)
          }

      case x: CachedProperty =>
        specifyType(CTAny.covariant, x)

      case x: CachedHasProperty =>
        specifyType(CTAny.covariant, x)

      // Check the variable is defined and, if not, define it so that later errors are suppressed
      // This is used in expressions; in graphs we must make sure to sem check variables explicitly (!)
      case x: Variable =>
        (s: SemanticState) =>
          s.ensureVariableDefined(x) match {
            case Right(ss) => SemanticCheckResult.success(ss)
            case Left(error) =>
              if (s.declareVariablesToSuppressDuplicateErrors) {
                // Most of the time we want to suppress if this error occurs again, by declaring the missing variable now
                s.declareVariable(x, CTAny.covariant) match {
                  // if the variable is a graph, declaring it will fail
                  case Right(ss)    => SemanticCheckResult.error(ss, error)
                  case Left(_error) => SemanticCheckResult.error(s, _error)
                }
              } else {
                // If we are ignoring errors anyway, the fake declaration might mess up the scope
                SemanticCheckResult.error(s, error)
              }
          }

      case x: FunctionInvocation =>
        SemanticFunctionCheck.check(ctx, x)

      case x: GetDegree =>
        check(ctx, x.node) chain
          expectType(CTMap.covariant | CTAny.invariant, x.node) chain
          specifyType(CTAny.covariant, x)

      case x: Parameter =>
        specifyType(x.parameterType.covariant, x)

      case x: ImplicitProcedureArgument =>
        specifyType(x.parameterType.covariant, x)

      case x: LabelExpressionPredicate =>
        check(ctx, x.entity) chain
          fromContext(semanticCheckContext =>
            when(
              semanticCheckContext.cypherVersion == CypherVersion.Cypher5 &&
                x.labelExpression.containsDynamicLabelOrTypeExpression
            ) {
              SemanticError.dynamicEntityTypeNotAllowed(x.position)
            }
          ) chain
          expectType(CTNode.covariant | CTRelationship.covariant, x.entity) chain
          checkLabelExpressionForLegacyRelationshipTypeDisjunction(x.entity, x.labelExpression) ifOkChain
          checkLabelExpression(None, x.labelExpression) chain
          specifyType(CTBoolean, x)

      case x: LabelOrTypeCheckExpression =>
        check(ctx, x.entityExpression) chain
          expectType(CTNode.covariant | CTRelationship.covariant, x.entityExpression) chain
          specifyType(CTBoolean, x) chain
          when(x.isInstanceOf[DynamicLabelsOrTypeExpressions]) {
            check(ctx, x.asInstanceOf[DynamicLabelsOrTypeExpressions].labelsOrTypes)
          }

      case x: LabelCheckExpression =>
        check(ctx, x.expression) chain
          expectType(CTNode.covariant, x.expression) chain
          specifyType(CTBoolean, x) chain
          when(x.isInstanceOf[DynamicLabelsExpressions]) {
            check(ctx, x.asInstanceOf[DynamicLabelsExpressions].labels)
          }

      case x: HasTypes =>
        check(ctx, x.expression) chain
          expectType(CTRelationship.covariant, x.expression) chain
          specifyType(CTBoolean, x)

      case x: HasDynamicType =>
        check(ctx, x.expression) chain
          check(ctx, x.types) chain
          expectType(CTRelationship.covariant, x.expression) chain
          specifyType(CTBoolean, x)

      case x: HasAnyDynamicType =>
        check(ctx, x.expression) chain
          check(ctx, x.types) chain
          expectType(CTRelationship.covariant, x.expression) chain
          specifyType(CTBoolean, x)

      // ITERABLES

      case x: ListComprehension =>
        FilteringExpressions.semanticCheck(ctx, x) chain
          checkInnerListComprehension(x)

      case x: PatternComprehension =>
        SemanticState.recordCurrentScope(x) chain
          withScopedState {
            SemanticPatternCheck.check(Pattern.SemanticContext.Match, x.pattern) chain
              x.namedPath.foldSemanticCheck(declareVariable(_, CTPath)) chain
              x.predicate.foldSemanticCheck(Where.checkExpression) chain
              simple(x.projection) chain
              SemanticState.recordCurrentScope(x.pattern)
          } chain {
            val outerTypes: TypeGenerator = types(x.projection)(_).wrapInList
            specifyType(outerTypes, x)
          }

      case _: FilterScope  => SemanticCheck.success
      case _: ExtractScope => SemanticCheck.success
      case _: ReduceScope  => SemanticCheck.success

      case x: CountStar =>
        specifyType(CTInteger, x) chain
          when(ctx == Expression.SemanticContext.Simple) {
            SemanticCheck.error(
              SemanticError.aggregateExpressionsNotAllowedInSimpleExpressions(
                x.asCanonicalStringVal,
                "count",
                x.position
              )
            )
          }

      case x: PathExpression =>
        specifyType(CTPath, x) chain
          check(ctx, x.step)

      case x: NodePathStep =>
        check(ctx, x.node) chain
          check(ctx, x.next)

      case x: SingleRelationshipPathStep =>
        check(ctx, x.rel) chain
          x.toNode.foldSemanticCheck(check(ctx, _)) chain
          check(ctx, x.next)

      case x: MultiRelationshipPathStep =>
        check(ctx, x.rel) chain
          x.toNode.foldSemanticCheck(check(ctx, _)) chain
          check(ctx, x.next)

      case x: RepeatPathStep =>
        check(ctx, x.variables.flatMap(_.variables)) chain
          check(ctx, x.toNode) chain
          check(ctx, x.next)

      case _: NilPathStep =>
        SemanticCheck.success

      case x: ShortestPathExpression =>
        SemanticPatternCheck.declareVariables(Pattern.SemanticContext.Expression)(x.pattern) chain
          SemanticPatternCheck.check(Pattern.SemanticContext.Expression)(x.pattern) chain
          when(x.pattern.element.folder.treeExists {
            case node: NodePattern => node.labelExpression.exists(_.containsGpmSpecificLabelExpression)
          }) {
            error(SemanticError.invalidLabelExpressionInShortestPath(x.position))
          } chain
          specifyType(if (x.pattern.single) CTPath else CTList(CTPath), x)

      case x: PatternExpression =>
        SemanticState.recordCurrentScope(x) chain
          withScopedState {
            // Check with Pattern.SemanticContext.Match so that we do not get "Variable not defined" error for new variables
            SemanticPatternCheck.check(Pattern.SemanticContext.Match, x.pattern) chain
              SemanticState.recordCurrentScope(x.pattern)
          } chain
          specifyType(CTList(CTPath), x) chain {
            // Check the relationshipChain again with Pattern.SemanticContext.Expression to generate errors for node and relationship predicates
            SemanticPatternCheck.check(Pattern.SemanticContext.Expression, x.pattern.element)
          }

      case x: IterablePredicateExpression =>
        FilteringExpressions.checkPredicateDefined(x) chain
          FilteringExpressions.semanticCheck(ctx, x) chain
          specifyType(CTBoolean, x)

      case x: ReduceExpression =>
        check(ctx, x.init) chain
          check(ctx, x.list) chain
          expectType(CTList(CTAny).covariant, x.list) chain
          withScopedState {
            val indexType: TypeGenerator = s =>
              (types(x.list)(s) constrain CTList(CTAny)).unwrapLists
            val accType: TypeGenerator = types(x.init)

            declareVariable(x.variable, indexType) chain
              declareVariable(x.accumulator, accType) chain
              simple(x.expression)
          } chain
          expectType(
            s => types(x.init)(s) coerceOrConvert types(x.expression)(s),
            x.expression,
            TypeMismatchContext.ACCUMULATOR,
            AccumulatorExpressionTypeMismatchMessageGenerator
          ) chain
          specifyType(s => types(x.init)(s) leastUpperBounds types(x.expression)(s), x)

      case x: ListLiteral =>
        def possibleTypes: TypeGenerator = state =>
          x.expressions match {
            case Seq() => CTList(CTAny).covariant
            case _     => leastUpperBoundsOfTypes(x.expressions)(state).wrapInCovariantList
          }

        check(ctx, x.expressions) chain specifyType(possibleTypes, x)

      case x: ListSlice =>
        check(ctx, x.list) chain
          expectType(CTList(CTAny).covariant, x.list) chain
          when(x.from.isEmpty && x.to.isEmpty) {
            SemanticError.emptyListRangeOperator(x.position)
          } chain
          check(ctx, x.from) chain
          expectType(CTInteger.covariant, x.from) chain
          check(ctx, x.to) chain
          expectType(CTInteger.covariant, x.to) chain
          specifyType(types(x.list), x)

      case x: ContainerIndex =>
        check(ctx, x.expr) chain
          check(ctx, x.idx) chain
          typeSwitch(x.expr) {
            // if we don't know the type of the container (e.g. it's given as a parameter) then we can't do a semantic check, instead it will blow up in runtime
            case exprT if !(exprT contains CTAny) =>
              typeSwitch(x.idx) {
                idxT =>
                  val listT = CTList(CTAny).covariant & exprT
                  val nodeT = CTNode.covariant & exprT
                  val relT = CTRelationship.covariant & exprT
                  val mapT = CTMap.invariant & exprT

                  val exprIsList = listT.nonEmpty
                  val exprIsNodeOrRel = nodeT.nonEmpty || relT.nonEmpty
                  val exprIsMap = mapT.nonEmpty

                  val idxIsInteger = (CTInteger.covariant & idxT).nonEmpty
                  val idxIsString = (CTString.covariant & idxT).nonEmpty

                  if (exprIsList) {
                    specifyType(listT.unwrapLists, x) chain
                      expectType(
                        CTInteger.covariant,
                        x.idx,
                        TypeMismatchContext.LIST_INDEX,
                        (_: String, actual: String) => s"list index must be given as Integer, but was $actual"
                      )
                  } else if (exprIsMap) {
                    expectType(
                      CTString.covariant,
                      x.idx,
                      TypeMismatchContext.MAP_KEY,
                      (_: String, actual: String) => s"map key must be given as String, but was $actual"
                    )
                  } else if (exprIsNodeOrRel) {
                    expectType(
                      CTString.covariant,
                      x.idx,
                      TypeMismatchContext.NODE_OR_RELATIONSHIP_PROPERTY_KEY,
                      (_: String, actual: String) =>
                        s"node or relationship property key must be given as String, but was $actual"
                    )
                  } else {
                    if (idxIsString) {
                      expectType(CTMap.covariant, x.expr)
                    } else if (idxIsInteger) {
                      expectType(CTList(CTAny).covariant, x.expr)
                    } else {
                      expectType(TypeSpec.union(CTMap.covariant, CTList(CTAny).covariant), x.expr)
                    }
                  }
              }
            case _ =>
              SemanticCheck.success
          }

      // MAPS

      case x: MapExpression =>
        check(ctx, x.items.map(_._2)) chain {
          (state: SemanticState) =>
            val entries =
              x.items
                .map { case (propKeyName, expr) =>
                  (propKeyName.name, state.expressionType(expr).specified)
                }
                .toMap
            state.specifyType(x, TypeSpecRange(CTMap, MapExtendedType(CTMap, entries)))
        }

      case x: MapProjection =>
        check(ctx, x.items) chain
          ensureDefined(x.name) chain
          expectType(CTMap.covariant, x.name) chain
          specifyType(CTMap, x) ifOkChain // We need to remember the scope to later rewrite this ASTNode
          SemanticState.recordCurrentScope(x)

      case x: LiteralEntry =>
        check(ctx, x.exp)

      case x: VariableSelector =>
        check(ctx, x.id)

      case _: PropertySelector =>
        SemanticCheck.success

      case _: AllPropertiesSelector =>
        SemanticCheck.success

      case x: DesugaredMapProjection =>
        check(ctx, x.items) chain
          check(ctx, x.entity) chain
          specifyType(CTMap, x) ifOkChain // We need to remember the scope to later rewrite this ASTNode
          SemanticState.recordCurrentScope(x)

      // LITERALS

      case x: StringDecimalInteger =>
        when(!validNumber(x)) {
          if (x.stringVal matches "^-?[1-9][0-9]*$") {
            SemanticError.numberTooLarge("integer", x.stringVal, x.position)
          } else {
            SemanticError.invalidLiteralNumber("decimal integer", x.stringVal, x.position)
          }
        } chain specifyType(CTInteger, x)

      case x: OctalIntegerLiteral =>
        val stringVal = x.stringVal
        when(!validNumber(x)) {
          if (stringVal matches "^-?0o?[0-7]+$") {
            SemanticError.numberTooLarge("integer", stringVal, x.position)
          } else {
            SemanticError.invalidLiteralNumber("octal integer", stringVal, x.position)
          }
        } ifOkChain {
          (state: SemanticState) =>
            {
              val errors =
                // old octal literal syntax, don't support underscores
                if (
                  stringVal.charAt(stringVal.indexOf('0') + 1) != 'o' && stringVal.charAt(
                    stringVal.indexOf('0') + 1
                  ) != '_'
                ) {
                  Seq(SemanticError.invalidOctalIntegerSyntax(stringVal, x.position))
                } else
                  Seq.empty[SemanticErrorDef]
              SemanticCheckResult(state, errors)
            }
        } chain specifyType(CTInteger, x)

      case x: HexIntegerLiteral =>
        val stringVal = x.stringVal
        when(!validNumber(x)) {
          if (stringVal matches "^-?0x[0-9a-fA-F]+$") {
            SemanticError.numberTooLarge("integer", x.stringVal, x.position)
          } else {
            SemanticError.invalidLiteralNumber("hex integer", stringVal, x.position)
          }
        } ifOkChain {
          (state: SemanticState) =>
            {
              val errors =
                if (stringVal.charAt(stringVal.indexOf('0') + 1) == 'X') {
                  Seq(SemanticError.invalidHexIntegerSyntax(stringVal, x.position))
                } else
                  Seq.empty[SemanticErrorDef]
              SemanticCheckResult(state, errors)
            }
        } chain specifyType(CTInteger, x)

      case x: DecimalDoubleLiteral =>
        when(!validNumber(x)) {
          SemanticError.invalidLiteralNumber("decimal double", x.stringVal, x.position)
        } ifOkChain
          when(x.value.isInfinite) {
            SemanticError.numberTooLarge("floating point number", x.stringVal, x.position)
          } chain specifyType(CTFloat, x)

      case x: StringLiteral =>
        specifyType(CTString, x)

      case x: Null =>
        specifyType(CTAny.covariant, x)

      case x: BooleanLiteral =>
        specifyType(CTBoolean, x)

      case x: Infinity =>
        specifyType(CTFloat, x)

      case x: NaN =>
        specifyType(CTFloat, x)

      case x: SemanticCheckableExpression =>
        x.semanticCheck(ctx)

      // EXISTS
      case x: ExistsExpression =>
        withScopedState {
          importValuesFromParentInExpressionWithScopeDependencies(x) chain
            fromState(state => x.query.semanticCheckInSubqueryExpressionContext(canOmitReturn = true, state)) chain
            when(x.query.containsUpdates) {
              SemanticError.anExpressionCannotContainUpdates("Exists", x.position)
            } chain
            when(x.query.endsWithFinish) {
              SemanticError.invalidEndOfQuery("An Exists Expression", x.position)
            } chain
            checkForShadowedVariables(
              x.query.folder.findAllByClass[ImportingWithSubqueryCall],
              x.query.folder.findAllByClass[ScopeClauseSubqueryCall]
            ) chain
            SemanticState.recordCurrentScope(x.query)
        } chain
          SemanticState.recordCurrentScope(x) chain
          specifyType(CTBoolean, x)

      // COUNT
      case x: CountExpression =>
        withScopedState {
          importValuesFromParentInExpressionWithScopeDependencies(x) chain
            fromState(state =>
              x.query.semanticCheckInSubqueryExpressionContext(
                canOmitReturn =
                  !x.query.isInstanceOf[UnionDistinct],
                state
              )
            ) chain
            when(x.query.containsUpdates) {
              SemanticError.aExpressionCannotContainUpdates("Count", x.position)
            } chain
            when(x.query.endsWithFinish) {
              SemanticError.invalidEndOfQuery("A Count Expression", x.position)
            } chain
            checkForShadowedVariables(
              x.query.folder.findAllByClass[ImportingWithSubqueryCall],
              x.query.folder.findAllByClass[ScopeClauseSubqueryCall]
            ) chain
            SemanticState.recordCurrentScope(x.query)
        } chain
          SemanticState.recordCurrentScope(x) chain
          specifyType(CTInteger, x)

      // COLLECT
      case x: CollectExpression =>
        withScopedState {
          importValuesFromParentInExpressionWithScopeDependencies(x) chain
            fromState(state => x.query.semanticCheckInSubqueryExpressionContext(canOmitReturn = false, state)) chain
            when(x.query.containsUpdates) {
              SemanticError.aExpressionCannotContainUpdates("Collect", x.position)
            } chain
            when(x.query.returnVariables.includeExisting || x.query.returnColumns.size != 1) {
              SemanticError.singleReturnColumnRequired(x.position)
              // by implication this also ensures that "A Collect Expression cannot contain a query ending with FINISH"
            } chain
            checkForShadowedVariables(
              x.query.folder.findAllByClass[ImportingWithSubqueryCall],
              x.query.folder.findAllByClass[ScopeClauseSubqueryCall]
            ) chain
            SemanticState.recordCurrentScope(x.query)
        } chain
          SemanticState.recordCurrentScope(x) chain
          specifyType(CTList(CTAny).covariant, x)

      case x: AllReducePredicate =>
        check(ctx, x.init) chain
          withScopedState {
            check(ctx, x.list) chain
              expectType(CTList(CTAny).covariant, x.list) chain
              declareVariable(x.accumulator, types(x.init)) chain
              withScopedState {
                importValuesFromParentInExpression(x.accumulator) chain
                  declareVariable(x.reductionStepVariable, unwrapLists(types(x.list)), overriding = true) chain
                  simple(x.reductionStep) chain
                  expectType(
                    s => types(x.init)(s),
                    x.reductionStep,
                    TypeMismatchContext.ACCUMULATOR,
                    AccumulatorReductionTypeMismatchMessageGenerator
                  ) chain
                  check(ctx, x.predicate) chain
                  expectType(CTBoolean.covariant, x.predicate) chain
                  specifyType(CTBoolean, x)
              }
          }

      case _: ObfuscatedLiteral => success

      // FALLBACK

      case x: Expression => semanticCheckFallback(ctx, x)
    }
  }

  private val stringifier = ExpressionStringifier()

  def checkLabelExpressionForLegacyRelationshipTypeDisjunction(
    entity: Expression,
    labelExpression: LabelExpression
  ): SemanticCheck =
    labelExpression.folder.treeFindByClass[ColonDisjunction]
      .foldSemanticCheck(disjunction => { (state: SemanticState) =>
        val isNode = state.expressionType(entity).actual == CTNode.invariant
        val sanitizedLabelExpression = stringifier.stringifyLabelExpression(labelExpression.replaceColonSyntax)
        SemanticCheckResult.error(
          state,
          SemanticError.legacyDisjunction(
            sanitizedLabelExpression,
            labelExpression.containsIs,
            isNode,
            disjunction.position
          )
        )
      })

  def checkLabelExpression(entityType: Option[EntityType], labelExpression: LabelExpression): SemanticCheck = {
    lazy val colonConjunctions = labelExpression.folder.findAllByClass[LabelExpression.ColonConjunction]
    lazy val colonDisjunctions = labelExpression.folder.findAllByClass[LabelExpression.ColonDisjunction]
    lazy val legacySymbols = colonConjunctions ++ colonDisjunctions
    when(entityType.contains(NODE_TYPE) && colonDisjunctions.nonEmpty) {
      SemanticCheck.error(SemanticError.invalidDisjunction(isNode = true, colonDisjunctions.head.position))
    } chain
      when(entityType.contains(RELATIONSHIP_TYPE) && colonConjunctions.nonEmpty) {
        SemanticCheck.error(SemanticError.invalidDisjunction(isNode = false, colonConjunctions.head.position))
      } ifOkChain
      when(
        // if we have a colon conjunction, this implies a node, so we can search the label expression with that in mind
        colonConjunctions.nonEmpty && labelExpression.containsGpmSpecificLabelExpression
      ) {
        val sanitizedLabelExpression = stringifier.stringifyLabelExpression(labelExpression.replaceColonSyntax)
        if (labelExpression.containsIs) {
          error(
            SemanticError.mixingColonAndIs(
              Set("IS " + stringifier.stringifyLabelExpression(labelExpression)),
              Set("IS " + sanitizedLabelExpression),
              legacySymbols.head.position
            )
          )
        } else
          error(
            SemanticError.invalidLabelExpression(Set(s":$sanitizedLabelExpression"), legacySymbols.head.position)
          )
      } chain {
        val tokenType = if (entityType.contains(RELATIONSHIP_TYPE)) TokenType.RelationshipType else TokenType.NodeLabel
        checkValidLabels(tokenType, labelExpression.flatten, labelExpression.position)
      }
  }

  /**
   * Build a semantic check over a iterable of expressions.
   * The simple semantic context disallows aggregating functions.
   */
  def simple(iterable: Iterable[Expression]): SemanticCheck = check(SemanticContext.Simple, iterable)

  def check(
    ctx: SemanticContext,
    iterable: Iterable[Expression]
  ): SemanticCheck =
    semanticCheckFold(iterable)(expr => check(ctx, expr))

  /**
   * Build a semantic check over an optional expression.
   * The simple semantic context disallows aggregating functions.
   */
  def simple(option: Option[Expression]): SemanticCheck = check(SemanticContext.Simple, option)

  def check(ctx: SemanticContext, option: Option[Expression]): SemanticCheck =
    option.foldSemanticCheck {
      check(ctx, _)
    }

  object FilteringExpressions {

    def semanticCheck(ctx: SemanticContext, e: FilteringExpression): SemanticCheck =
      SemanticExpressionCheck.check(ctx, e.expression) chain
        expectType(CTList(CTAny).covariant, e.expression) ifOkChain
        checkInnerPredicate(e)

    def checkPredicateDefined(e: FilteringExpression): SemanticCheck =
      when(e.innerPredicate.isEmpty) {
        SemanticError.functionRequiresWhereClause(e.name, e.position)
      }

    private def checkInnerPredicate(e: FilteringExpression): SemanticCheck =
      e.innerPredicate match {
        case Some(predicate) => withScopedState {
            declareVariable(e.variable, possibleInnerTypes(e)) chain
              SemanticExpressionCheck.simple(predicate) chain
              SemanticExpressionCheck.expectType(CTBoolean.covariant, predicate)
          }
        case None => SemanticCheck.success
      }

    def possibleInnerTypes(e: FilteringExpression): TypeGenerator = s =>
      (types(e.expression)(s) constrain CTList(CTAny)).unwrapLists
  }

  private val allSimpleTypes = CTBoolean.covariant | CTString.covariant | CTInteger.covariant | CTFloat.covariant |
    CTDate.covariant | CTLocalTime.covariant | CTTime.covariant | CTLocalDateTime.covariant | CTDateTime.covariant |
    CTDuration.covariant | CTPoint.covariant | CTVector.covariant

  private def infixAddRhsTypes(lhs: Expression): TypeGenerator = s => {
    val lhsTypes = types(lhs)(s)

    // Strings
    // "a" + "b" => "ab"
    // "a" + 1 => "a1"
    // "a" + 1.1 => "a1.1"
    // "a" + T => "axx"

    // Numbers
    // 1 + "b" => "1b"
    // 1 + 1 => 2
    // 1 + 1.1 => 2.1
    // 1.1 + "b" => "1.1b"
    // 1.1 + 1 => 2.1
    // 1.1 + 1.1 => 2.2

    // Temporals
    // T + Duration => T
    // Duration + T => T
    // Duration + Duration => Duration

    // Other types
    // T + "b" => "xxb"

    val stringTypes =
      if (lhsTypes containsAny CTString.covariant) {
        allSimpleTypes
      } else {
        TypeSpec.none
      }

    val numberTypes =
      if (lhsTypes containsAny (CTInteger.covariant | CTFloat.covariant)) {
        CTString.covariant | CTInteger.covariant | CTFloat.covariant
      } else {
        TypeSpec.none
      }
    val temporalTypes =
      if (
        lhsTypes containsAny (CTDate.covariant | CTTime.covariant | CTLocalTime.covariant |
          CTDateTime.covariant | CTLocalDateTime.covariant | CTDuration.covariant)
      ) {
        CTString.covariant | CTDuration.covariant
      } else {
        TypeSpec.none
      }
    val durationTypes =
      if (lhsTypes containsAny CTDuration.covariant) {
        CTString.covariant | CTDate.covariant | CTTime.covariant | CTLocalTime.covariant |
          CTDateTime.covariant | CTLocalDateTime.covariant | CTDuration.covariant
      } else {
        TypeSpec.none
      }

    val otherTypes =
      if (lhsTypes containsAny (CTBoolean.covariant | CTPoint.covariant | CTVector.covariant)) {
        CTString.covariant
      } else {
        TypeSpec.none
      }

    // [a] + [b] => [a, b]
    val listTypes = (lhsTypes leastUpperBounds CTList(CTAny) constrain CTList(CTAny)).covariant

    // [a] + b => [a, b]
    val lhsListTypes = listTypes | listTypes.unwrapLists

    // a + [b] => [a, b]
    val rhsListTypes = CTList(CTAny).covariant

    stringTypes | numberTypes | lhsListTypes | rhsListTypes | temporalTypes | durationTypes | otherTypes
  }

  private def infixAddOutputTypes(lhs: Expression, rhs: Expression): TypeGenerator = s => {
    val lhsTypes = types(lhs)(s)
    val rhsTypes = types(rhs)(s)

    def when(fst: TypeSpec, snd: TypeSpec)(result: CypherType): TypeSpec =
      if (
        lhsTypes.containsAny(fst) && rhsTypes.containsAny(snd) || lhsTypes.containsAny(snd) && rhsTypes.containsAny(fst)
      ) {
        result.invariant
      } else {
        TypeSpec.none
      }
    // "a" + "b" => "ab"
    // "a" + 1 => "a1"
    // "a" + 1.1 => "a1.1"
    // "a" + T => "axx"
    // 1 + "b" => "1b"
    // 1.1 + "b" => "1.1b"
    // T + "b" => "xxb"
    val stringTypes: TypeSpec =
      when(CTString.covariant, allSimpleTypes)(CTString)

    // 1 + 1 => 2
    // 1 + 1.1 => 2.1
    // 1.1 + 1 => 2.1
    // 1.1 + 1.1 => 2.2
    val numberTypes: TypeSpec =
      when(CTInteger.covariant, CTInteger.covariant)(CTInteger) |
        when(CTFloat.covariant, CTFloat.covariant | CTInteger.covariant)(CTFloat)

    val temporalTypes: TypeSpec =
      when(CTDuration.covariant, CTDuration.covariant)(CTDuration) |
        when(CTDate.covariant, CTDuration.covariant)(CTDate) |
        when(CTDuration.covariant, CTDate.covariant)(CTDate) |
        when(CTTime.covariant, CTDuration.covariant)(CTTime) |
        when(CTDuration.covariant, CTTime.covariant)(CTTime) |
        when(CTLocalTime.covariant, CTDuration.covariant)(CTLocalTime) |
        when(CTDuration.covariant, CTLocalTime.covariant)(CTLocalTime) |
        when(CTLocalDateTime.covariant, CTDuration.covariant)(CTLocalDateTime) |
        when(CTDuration.covariant, CTLocalDateTime.covariant)(CTLocalDateTime) |
        when(CTDateTime.covariant, CTDuration.covariant)(CTDateTime) |
        when(CTDuration.covariant, CTDateTime.covariant)(CTDateTime)

    val listTypes = {
      val lhsListTypes = lhsTypes constrain CTList(CTAny)
      val rhsListTypes = rhsTypes constrain CTList(CTAny)
      val lhsListInnerTypes = lhsListTypes.unwrapLists
      val rhsListInnerTypes = rhsListTypes.unwrapLists
      val lhsScalarTypes = lhsTypes without CTList(CTAny)
      val rhsScalarTypes = rhsTypes without CTList(CTAny)

      val bothListMergedLhTypes = (lhsListInnerTypes coerceOrLeastUpperBound rhsListInnerTypes).wrapInList
      val bothListMergedRhTypes = (rhsListInnerTypes coerceOrLeastUpperBound lhsListInnerTypes).wrapInList

      val lhListMergedTypes = (rhsScalarTypes coerceOrLeastUpperBound lhsListInnerTypes).wrapInList
      val rhListMergedTypes = (lhsScalarTypes coerceOrLeastUpperBound rhsListInnerTypes).wrapInList

      bothListMergedLhTypes | bothListMergedRhTypes | lhListMergedTypes | rhListMergedTypes
    }

    stringTypes | numberTypes | listTypes | temporalTypes
  }

  private def checkInnerListComprehension(x: ListComprehension): SemanticCheck =
    x.extractExpression match {
      case Some(e) =>
        withScopedState {
          declareVariable(x.variable, FilteringExpressions.possibleInnerTypes(x)) chain
            simple(e)
        } chain {
          val outerTypes: TypeGenerator = types(e)(_).wrapInList
          specifyType(outerTypes, x)
        }
      case None => withScopedState {
          // Even if there is no usage of that variable, we need to declare it, to not confuse the Namespacer
          declareVariable(x.variable, FilteringExpressions.possibleInnerTypes(x))
        } chain {
          specifyType(types(x.expression), x)
        }
    }

  private def checkForShadowedVariables(
    importingWithSubqueryCallsToFilter: Seq[ImportingWithSubqueryCall],
    scopeClauseSubqueryCallsToFilter: Seq[ScopeClauseSubqueryCall]
  ): SemanticCheck = (inner: SemanticState) => {
    // Only check the first time to avoid unnecessary checks after extensive rewriting.
    if (inner.semanticCheckHasRunOnce) SemanticCheckResult(inner, Seq.empty)
    else {
      val outerScopeSymbols = inner.currentScope.parent.get.parent match {
        case Some(parent) => parent.scope.symbolTable ++ inner.currentScope.parent.get.scope.symbolTable
        case None         => inner.currentScope.parent.get.scope.symbolTable
      }
      val innerScopeSymbols: Map[String, Set[Symbol]] = inner.currentScope.scope.allSymbols

      // Variables inside CALL {} are allowed to shadow outside variables and should not be considered errors.
      val importingWithSubqueryCallScopes =
        inner.recordedScopes.filter(recordedScope => importingWithSubqueryCallsToFilter.contains(recordedScope._1.node))
      val importingWithSubqueryCallSymbols = importingWithSubqueryCallScopes.map { case (_, scope) =>
        scope.parent.map(_.scope.allSymbols).getOrElse(Map.empty)
      }.flatten.groupMapReduce(_._1)(_._2)(_ ++ _)

      // Variables inside CALL () {} are allowed to shadow outside variables and should not be considered errors.
      val scopedClauseSubqueryCallScopes =
        inner.recordedScopes.filter(recordedScope => scopeClauseSubqueryCallsToFilter.contains(recordedScope._1.node))
      val returnsOfSubquery = scopeClauseSubqueryCallsToFilter.flatMap(_.innerQuery.returnVariables.explicitVariables)
      val returnVariableNames = returnsOfSubquery.map(_.name)
      val scopeClauseSubqueryCallSymbols = scopedClauseSubqueryCallScopes.map { case (_, scope) =>
        scope.parent.map(_.scope.allSymbols).getOrElse(Map.empty)
      }.flatten.groupMapReduce(_._1)(_._2)(_ ++ _)

      val innerScopeSymbolsWithoutSubquerySymbols: Map[String, Set[Symbol]] =
        innerScopeSymbols
          .filterNot { case (x, _) => scopeClauseSubqueryCallSymbols.exists(_._1 == x) }
          .filterNot { case (x, y) => importingWithSubqueryCallSymbols.get(x).contains(y) }

      // Union symbols are excluded as those always have the position of the UNION keyword regardless of shadowing
      val innerDefinitions = innerScopeSymbolsWithoutSubquerySymbols.map {
        case (name, symbols) =>
          (name, symbols.filterNot(symbol => symbol.unionSymbol).map(_.definition))
      }

      val innerVariables = innerDefinitions.values.flatMap(x => x.map(_.asVariable)) ++ returnsOfSubquery

      // If a variable of the same name exists in the inner scope and it is not a reference to the outer scope variable.
      // Also the outer variable must come before the inner variable (i.e. have a lower position) for it to be a case of shadowing.
      def isShadowed(s: Symbol): Boolean = {
        val outerName = s.name
        val outerPos = s.definition.positionsAndUniqueIdString._1

        (innerDefinitions.contains(outerName) &&
          innerDefinitions(outerName).exists(inner => inner.positionsAndUniqueIdString._1 > outerPos)) ||
        returnVariableNames.contains(outerName)
      }

      val shadowedSymbols: Map[String, InputPosition] = outerScopeSymbols.collect {
        // Return variables only need a name match, but inner definitions we need to make sure we don't collect
        // the wrong position
        case (name, symbol)
          if innerDefinitions.contains(name) && innerDefinitions(name).exists(_ != symbol.definition) && isShadowed(
            symbol
          ) =>
          name -> innerDefinitions(name).find(_ != symbol.definition).get.asVariable.position
        case (name, symbol) if isShadowed(symbol) =>
          name -> innerVariables.find(_.name == name).get.position
      }

      val errors = shadowedSymbols.map {
        case (varName, pos) =>
          SemanticError.variableShadowingOuterScope(varName, pos)
      }.toSeq

      SemanticCheckResult(inner, errors)
    }
  }

  private def literalShouldBeNumberInRange(
    expression: Expression,
    name: String,
    lowerBound: Integer,
    upperBound: Integer
  ): SemanticCheck = {
    try {
      expression match {
        case i: IntegerLiteral if i.value >= lowerBound && i.value <= upperBound        => SemanticCheck.success
        case i: DoubleLiteral if i.value >= 0.0d && i.value <= upperBound.doubleValue() => SemanticCheck.success
        case lit: NumberLiteral =>
          SemanticAnalysisToolingErrorWithGqlInfo.specifiedNumberOutOfRangeError(
            name,
            "NUMBER",
            lowerBound,
            upperBound,
            lit.asCanonicalStringVal,
            s"Invalid input. '${lit.asCanonicalStringVal}' is not a valid value. Must be a number in the range $lowerBound to $upperBound.",
            lit.position
          )
        case _ => SemanticCheck.success
      }
    } catch {
      case _: NumberFormatException =>
        // We rely on getting a SemanticError from Type checking otherwise
        SemanticCheck.success
    }
  }
}
