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
package org.neo4j.cypher.internal.frontend.local_callables

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.semantics.FeatureError
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.LocalCallables
import org.neo4j.cypher.internal.frontend.NameWithPositionCaretBasedSemanticAnalysisTestSuite
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QueryLanguage
import org.neo4j.cypher.internal.frontend.phases.QueryLanguage.Cypher25
import org.neo4j.cypher.internal.frontend.phases.ScopedProcedureSignatureResolver
import org.neo4j.cypher.internal.frontend.phases.TryResolveCallables
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ResolveLocalFunctions
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ResolveLocalProceduresStep1
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ResolveLocalProceduresStep2
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.messages.MessageUtilProvider

trait LocalCallablesSemanticAnalysisTest
    extends CypherFunSuite
    with NameWithPositionCaretBasedSemanticAnalysisTestSuite with AstConstructionTestSupport {

  override def messageProvider: ErrorMessageProvider = MessageUtilProvider

  def errorFeatureRequired(offset: Int, line: Int, column: Int): FeatureError =
    FeatureError.notAvailableInThisImplementation(
      LocalCallables,
      "The DEFINE keyword",
      InputPosition(offset, line, column)
    )

  def errorFeatureRequired(p: InputPosition): FeatureError =
    FeatureError.notAvailableInThisImplementation(
      LocalCallables,
      "The DEFINE keyword",
      p
    )

  private val makeResolverMock: ScopedProcedureSignatureResolver = {
    val name = procedureName("my", "proc42", "foo")
    val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
    val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))

    val signature =
      ProcedureSignature(name, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess, id = 42)

    val procSignatureLookup: ProcedureName => ProcedureSignature = _ => signature
    val funcSignatureLookup: FunctionName => Option[UserFunctionSignature] = _ => None

    new ScopedProcedureSignatureResolver {
      override def procedureSignature(n: ProcedureName): ProcedureSignature = procSignatureLookup(n)

      override def functionSignature(n: FunctionName): Option[UserFunctionSignature] = funcSignatureLookup(n)

      override def procedureSignatureVersion: Long = 42

      override def queryLanguage: QueryLanguage = Cypher25

      override def functionSignatureInOtherVersion(name: FunctionName): Option[UserFunctionSignature] = None
    }
  }

  def runWithoutLC(): AnalysisAssertions = {
    runWith(disabledCypherVersions = Set(CypherVersion.Cypher5))
  }

  def runWithLC(features: SemanticFeature*): AnalysisAssertions = {
    runWith(
      disabledCypherVersions = Set(CypherVersion.Cypher5),
      semanticAnalysisTwice(
        extraStepBefore = Some(
          ScopeSurveyor andThen
            ResolveLocalFunctions andThen
            ResolveLocalProceduresStep1 andThen
            ResolveLocalProceduresStep2 andThen
            TryResolveCallables(makeResolverMock)
        )
      ),
      features ++ Seq(LocalCallables): _*
    )
  }

  def msg22N27(expected: String, actual: String): String =
    s"Type mismatch: expected $expected but was $actual"

  def msg22NB1(expected: String, actual: String): String =
    s"Type mismatch: expected $expected but was $actual"

  def msg42I13_tooFew(name: String): String =
    s"Insufficient parameters for function '$name'"

  def msg42I13_tooMany(name: String): String =
    s"Too many parameters for function '$name'"

  def msg42N21(context: String): String =
    s"Expression in $context must be aliased (use AS)"

  def msg42N25(): String =
    s"Procedure call inside a query does not support naming results implicitly (name explicitly using `YIELD` instead)"

  def msg42N39(): String =
    s"All sub queries in an UNION must have the same return column names"

  def msg42N50(column: String): String =
    s"Unknown procedure output: `$column`"

  def msg42N57(): String =
    s"Local function cannot contain any updates"

  def msg42N59(column: String): String =
    s"Variable `$column` already declared"

  def msg42NAH(column: String, procedureName: String): String =
    s"Return column `$column` does not match output signature of local procedure $procedureName"

  def msg42NAI(column: String, procedureName: String): String =
    s"Return column `$column` is missing to match output signature of local procedure $procedureName"

  def msg42NAJ(typeName: String, column: String, procedureName: String): String =
    s"`$typeName` is not supported as local procedure output type. Adjust the type of output field `$column` of local procedure $procedureName"

  def msg42NAK(typeName: String, functionName: String): String =
    s"`$typeName` is not supported as local function return type. Adjust the return type of local function $functionName"

  def msg42NAL(typeName: String, column: String, procedureName: String): String =
    s"`$typeName` is not supported as local callable parameter type. Adjust the type of parameter `$column` of local callable $procedureName"

  def msg42NAN(functionName: String): String =
    s"Non-scalar query result not supported in local function definitions. " +
      s"Query in local function definitions $functionName requires a `RETURN` clause with a single column and computing a total aggregate or containing `LIMIT 1`."

  def msg42I42(): String =
    s"Cannot yield value from void procedure."

  def msg42NAO(): String =
    s"'CALL { ... } IN TRANSACTIONS' is not supported in combination with 'DEFINE'"
}
