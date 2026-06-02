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
package org.neo4j.cypher.internal.frontend.scoping.surveyor.inspection_tool

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.ConditionalQueryBranch
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.GroupBy
import org.neo4j.cypher.internal.ast.LocalCallableDefinition
import org.neo4j.cypher.internal.ast.Search
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.ScopeQueries
import org.neo4j.cypher.internal.ast.semantics.scoping.CommonContext
import org.neo4j.cypher.internal.ast.semantics.scoping.Declarations
import org.neo4j.cypher.internal.ast.semantics.scoping.ExpressionResult
import org.neo4j.cypher.internal.ast.semantics.scoping.LocalCallableScopeSignature
import org.neo4j.cypher.internal.ast.semantics.scoping.NoResult
import org.neo4j.cypher.internal.ast.semantics.scoping.OmittedResult
import org.neo4j.cypher.internal.ast.semantics.scoping.PatternIncomingContext
import org.neo4j.cypher.internal.ast.semantics.scoping.PatternScope
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionExpressionContext
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionItem
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionPart
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionSpecification
import org.neo4j.cypher.internal.ast.semantics.scoping.References
import org.neo4j.cypher.internal.ast.semantics.scoping.RegularContext
import org.neo4j.cypher.internal.ast.semantics.scoping.Result
import org.neo4j.cypher.internal.ast.semantics.scoping.TableResult
import org.neo4j.cypher.internal.ast.semantics.scoping.TableResultWithNotYetKnownColumns
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingContext
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.Parse
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NotImplementedErrorMessageProvider

import java.awt.Desktop
import java.net.HttpURLConnection
import java.net.URI
import java.net.URL
import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.annotation.nowarn

import scalatags.Text.all.*

/**
 * Small inspection tool for exploring the `WorkingScope` tree produced for a Cypher query.
 *
 * Run this object to start a local HTTP server, open the browser UI (at http://localhost:8080/),
 * enter a query, and inspect the rendered scope tree with client-side expand/collapse of nested scopes.
 */
object WorkingScopeInspector extends cask.MainRoutes {

  private val version = CypherVersion.Cypher25
  private val messageProvider: ErrorMessageProvider = NotImplementedErrorMessageProvider
  private val prettifier: Prettifier = Prettifier(ExpressionStringifier())
  private val serverUri = URI.create("http://localhost:8080/")

  private val sampleQuery =
    """MATCH (n)-[r]->(m)
      |WITH n, r, m
      |RETURN n, count(r) AS relCount""".stripMargin

  override def host: String = "localhost"
  override def port: Int = 8080
  override def debugMode: Boolean = true

  // `@cask.get` expands to routing code that triggers a spurious non-exhaustive match warning under `-Werror`.
  @nowarn("msg=match may not be exhaustive")
  @cask.get("/")
  def index(query: String = ""): cask.Response[String] = {
    htmlResponse(page(query))
  }

  // `@cask.get` expands to routing code that triggers a spurious non-exhaustive match warning under `-Werror`.
  @nowarn("msg=match may not be exhaustive")
  @cask.get("/inspect")
  def inspect(query: String): cask.Response[String] = {
    htmlResponse(renderInspection(query))
  }

  override def main(args: Array[String]): Unit = {
    val serverThread = new Thread(
      new Runnable {
        override def run(): Unit = WorkingScopeInspector.super.main(args)
      },
      "working-scope-inspector-server"
    )
    serverThread.setDaemon(false)
    serverThread.start()
    awaitServerReady(serverUri)
    println(s"Working Scope Inspector is ready at $serverUri")
    openBrowser(serverUri)
    serverThread.join()
  }

  initialize()

  private def page(query: String): String =
    "<!doctype html>" + html(
      head(
        meta(charset := "utf-8"),
        meta(name := "viewport", content := "width=device-width, initial-scale=1"),
        link(rel := "stylesheet", href := "https://fonts.googleapis.com/css?family=Public Sans"),
        link(rel := "stylesheet", href := "https://fonts.googleapis.com/css?family=Nunito Sans"),
        link(rel := "stylesheet", href := "https://fonts.googleapis.com/css?family=Helvetica Neue"),
        link(rel := "stylesheet", href := "https://fonts.googleapis.com/css?family=Fira Code"),
        scalatags.Text.tags2.title("WorkingScopeInspector"),
        scalatags.Text.tags2.style(raw(styles))
      ),
      body(
        scalatags.Text.tags2.main(cls := "page")(
          scalatags.Text.tags2.section(cls := "controls")(
            h1("Working Scope Inspector"),
            p(cls := "subtitle")("Inspect the WorkingScope tree for a Cypher query."),
            form(id := "queryForm", cls := "query-form")(
              label(`for` := "queryInput", cls := "field-label")("Cypher query"),
              textarea(
                id := "queryInput",
                name := "query",
                rows := 10,
                spellcheck := "false",
                placeholder := "Enter a Cypher query"
              )(if (query.nonEmpty) query else sampleQuery),
              div(cls := "actions")(
                button(`type` := "submit", cls := "primary")("Inspect")
              )
            )
          ),
          scalatags.Text.tags2.section(
            id := "result",
            cls := "result-panel",
            attr("data-has-content") := query.nonEmpty.toString
          )(
            if (query.nonEmpty) raw(renderInspection(query))
            else div(cls := "placeholder")("Submit a query to inspect its WorkingScope tree.")
          )
        ),
        script(raw(scriptSource))
      )
    ).render

  private def renderInspection(query: String): String = {
    val trimmed = query.trim
    if (trimmed.isEmpty) {
      div(cls := "placeholder")("Enter a query to inspect.").render
    } else {
      inspectScope(trimmed) match {
        case InspectionSuccess(scope, warnings) =>
          div(
            cls := "inspection-root",
            if (warnings.nonEmpty) div(cls := "error-block warning")(renderWarningList(warnings)) else (),
            renderScopeNode(scope, expanded = true)
          ).render
        case InspectionFailure(message, details) =>
          div(
            cls := "error-block",
            h2("Inspection failed"),
            p(message),
            if (details.nonEmpty) pre(cls := "detail-block")(details.mkString("\n")) else ()
          ).render
      }
    }
  }

  private def inspectScope(query: String): InspectionResult = {
    val context =
      new ErrorCollectingContext(version, semanticFeatures = Seq(ScopeQueries)) {
        override def errorMessageProvider: ErrorMessageProvider = messageProvider
      }
    val transformer = Parse andThen ScopeSurveyor
    val initialState = InitialState(query, NoPlannerName, new AnonymousVariableNameGenerator)

    try {
      val state = transformer.transform(initialState, context)
      val workingScopeOpt = state.maybeScopeState.map(_.workingScope)
      val errors = context.errors.map(_.msg)
      workingScopeOpt match {
        case Some(workingScope) => InspectionSuccess(workingScope, errors)
        case None =>
          InspectionFailure(
            "No WorkingScope was produced for the query.",
            errors match {
              case Seq() => Seq("The scope state was empty after Parse and ScopeSurveyor.")
              case xs    => xs
            }
          )
      }
    } catch {
      case e: Throwable =>
        InspectionFailure(
          Option(e.getMessage).getOrElse(e.getClass.getSimpleName),
          e.getStackTrace.iterator.map(_.toString).take(25).toSeq
        )
    }
  }

  private def renderScopeNode(scope: WorkingScope, expanded: Boolean): Frag = {
    val nodeId = "scope-" + System.identityHashCode(scope)
    div(
      cls := "scope-node",
      attr("data-expanded") := expanded.toString
    )(
      div(cls := "scope-shell")(
        div(cls := "scope-summary", attr("data-toggle-scope") := "true")(
          div(cls := "scope-summary-main")(
            div(cls := "scope-kind")(camelCaseToLowerCaseWithSpaces(scope.getClass.getSimpleName)),
            pre(cls := "scope-ast-preview")(prettify(scope.astNode))
          ),
          button(
            cls := "toggle-button",
            attr("data-target-scope") := nodeId,
            `type` := "button",
            attr("aria-label") := "Toggle scope details"
          )()
        ),
        div(id := nodeId, cls := "scope-body")(
          div(cls := "scope-grid")(
            renderContextAttribute(extractIncoming(scope)),
            renderReferencedAttribute(scope.referenced),
            renderDeclarationsAttribute("declared", scope.declared),
            renderOutgoingAttribute(scope.outgoing),
            renderResultAttribute(scope.result)
          ),
          if (scope.children.nonEmpty)
            div(
              cls := "children-section"
            )(
              div(cls := "children-heading")(s"children (${scope.children.size})"),
              div(cls := "children-list")(scope.children.map(child => renderScopeNode(child, expanded = false)))
            )
          else
            div(cls := "children-empty")("No children")
        )
      )
    )
  }

  private def extractIncoming(scope: WorkingScope): WorkingContext = scope match {
    case PatternScope(_, patternIncoming, _, _, _, _) => patternIncoming
    case other                                        => other.incoming
  }

  private def renderContextAttribute(incoming: WorkingContext): Frag =
    incoming match {
      case patternIncomingContext: PatternIncomingContext =>
        renderPatternIncomingContextAttribute("PatternIncoming", patternIncomingContext)
      case regularContext: RegularContext =>
        renderRegularContextAttribute("RegularIncoming", regularContext)
      case workingContext =>
        renderAttribute("Incoming", workingContext.getClass.getSimpleName)
    }

  private def renderOutgoingAttribute(outgoing: RegularContext): Frag =
    renderRegularContextAttribute("outgoing", outgoing)

  private def renderPatternIncomingContextAttribute(labelText: String, context: PatternIncomingContext): Frag = {
    renderAttributeBlock(
      labelText,
      Seq(
        renderValueList("topologicalConstants", context.topologicalConstants),
        renderValueList("predicateConstants", context.predicateConstants),
        renderValueList("pathConstants", context.pathConstants),
        renderValueList("groupConstants", context.groupConstants),
        renderCallableList("localCallables", context.localCallables)
      )
    )
  }

  private def renderRegularContextAttribute(labelText: String, context: RegularContext): Frag = {
    val normalized = context match {
      case cc: CommonContext               => cc
      case pc: ProjectionExpressionContext => CommonContext(pc.constants, pc.variables, pc.localCallables)
    }
    val (updatedLabelText, projectionRows) = context match {
      case pc: ProjectionExpressionContext =>
        (
          "ProjectionExpressionIncoming",
          Seq(
            renderProjectionPart("projectionPart", pc.projectionPart),
            renderProjectionSpecification("projectionSpecification", pc.projectionSpecification)
          )
        )
      case _ => (labelText, Seq.empty)
    }
    renderAttributeBlock(
      updatedLabelText,
      Seq(
        renderValueList("constants", normalized.constants),
        renderValueList("variables", normalized.variables),
        renderCallableList("localCallables", normalized.localCallables)
      ) ++ projectionRows
    )
  }

  private def renderReferencedAttribute(references: References): Frag =
    renderAttributeBlock(
      "referenced",
      Seq(renderValueList("variables", references.references.keysIterator.map(_.value).distinct.toSeq))
    )

  private def renderDeclarationsAttribute(labelText: String, declarations: Declarations): Frag =
    renderAttributeBlock(
      labelText,
      Seq(
        renderValueList("constants", declarations.constants),
        renderValueList("variables", declarations.variables),
        renderCallableList("localCallables", declarations.localCallables)
      )
    )

  private def renderResultAttribute(result: Result): Frag = {
    val values =
      result match {
        case TableResult(columns) =>
          Seq(
            renderScalar("type", getClassNameWithoutDollarSignOnClassName(result)),
            renderValueList("columns", columns)
          )
        case TableResultWithNotYetKnownColumns =>
          Seq(renderScalar("type", getClassNameWithoutDollarSignOnClassName(result)))
        case OmittedResult =>
          Seq(renderScalar("type", getClassNameWithoutDollarSignOnClassName(result)))
        case NoResult =>
          Seq(renderScalar("type", getClassNameWithoutDollarSignOnClassName(result)))
        case ExpressionResult =>
          Seq(renderScalar("type", getClassNameWithoutDollarSignOnClassName(result)))
        case other =>
          Seq(renderScalar("type", getClassNameWithoutDollarSignOnClassName(other)))
      }
    renderAttributeBlock("result", values)
  }

  private def getClassNameWithoutDollarSignOnClassName(x: Any, t: String => String = identity): String = {
    val className = x.getClass.getSimpleName
    if (className.endsWith("$")) {
      className.init
    } else {
      className
    }
  }

  private def camelCaseToLowerCaseWithSpaces(value: String): String =
    value
      .replaceAll("([a-z0-9])([A-Z])", "$1 $2")
      .replaceAll("([A-Z])([A-Z][a-z])", "$1 $2")
      .toLowerCase(Locale.ROOT)

  private def renderAttribute(labelText: String, value: String, preserveWhitespace: Boolean = false): Frag =
    div(cls := "attribute-card")(
      div(cls := "attribute-label")(labelText),
      if (preserveWhitespace) pre(cls := "attribute-value preformatted")(value)
      else div(cls := "attribute-value")(if (value.nonEmpty) value else "-")
    )

  private def renderAttributeBlock(labelText: String, rows: Seq[Frag]): Frag =
    div(cls := "attribute-card")(
      div(cls := "attribute-label")(camelCaseToLowerCaseWithSpaces(labelText)),
      div(cls := "attribute-block")(rows)
    )

  private def renderScalar(labelText: String, value: Frag): Frag =
    div(cls := "kv-row")(
      div(cls := "kv-key")(camelCaseToLowerCaseWithSpaces(labelText)),
      div(cls := "kv-value")(value)
    )

  private def renderProjectionPart(labelText: String, projectionPart: ProjectionPart): Frag =
    renderAttributeBlock(
      labelText,
      Seq(
        renderScalar("type", getClassNameWithoutDollarSignOnClassName(projectionPart)),
        renderScalar("isSubclause", projectionPart.isSubclause.toString)
      )
    )

  private def renderProjectionSpecification(
    labelText: String,
    projectionSpecification: ProjectionSpecification
  ): Frag =
    renderAttributeBlock(
      labelText,
      Seq(
        renderProjectionItemList("groupingKeys", projectionSpecification.groupingKeys),
        renderProjectionItemList("nonAggregatingItems", projectionSpecification.nonAggregatingItems),
        renderProjectionItemList("aggregatingItems", projectionSpecification.aggregatingItems),
        renderScalar("distinct", projectionSpecification.distinct.toString),
        renderScalar("hasGroupBy", projectionSpecification.hasGroupBy.toString)
      )
    )

  private def joinFragsWithBreaks(values: Seq[Frag]): Frag =
    frag(values.flatMap(value => Seq(br(), value)).drop(1))

  private def renderValueList(labelText: String, values: Iterable[LogicalVariable]): Frag = {
    val formatted = values.toSeq.sortBy(v => (v.position.offset, v.name)).map(formatVariable)
    renderScalar(labelText, joinFragsWithBreaks(formatted))
  }

  private def renderCallableList(labelText: String, values: Iterable[LocalCallableScopeSignature]): Frag = {
    val formatted = values.toSeq
      .sortBy(callable => callable.name.fullName)
      .map(formatCallable)
    renderScalar(labelText, joinFragsWithBreaks(formatted))
  }

  private def renderProjectionItemList(labelText: String, values: Iterable[ProjectionItem]): Frag = {
    val formatted = values.iterator.toSeq
      .sortBy(item => (item.expression.position.offset, item.alias.map(_.name).getOrElse("")))
      .map(formatProjectionItem)
    renderScalar(labelText, if (formatted.nonEmpty) joinFragsWithBreaks(formatted) else "-")
  }

  private def renderWarningList(warnings: Seq[String]): Frag =
    div(
      h2("Frontend reported errors"),
      pre(cls := "detail-block")(warnings.sorted.mkString("\n"))
    )

  private def formatVariable(variable: LogicalVariable): Frag =
    frag(
      s"${variable.name}",
      span(cls := "variablePosition")(s"@${variable.position.offset}")
    )

  private def formatCallable(callable: LocalCallableScopeSignature): Frag =
    frag(s"${callable.name.fullName} : ${formatResult(callable.result)}")

  private def formatProjectionItem(item: ProjectionItem): Frag =
    frag(
      prettifier.expr(item.expression),
      item.alias.map(alias => frag(" AS ", formatVariable(alias))).getOrElse(frag())
    )

  private def formatResult(result: Result): String =
    result match {
      case TableResult(columns) =>
        s"${result.getClass.getSimpleName}(${columns.iterator.map(formatVariable).mkString(", ")})"
      case other =>
        other.getClass.getSimpleName
    }

  private def htmlResponse(body: String): cask.Response[String] =
    cask.Response(
      data = body,
      headers = Seq("Content-Type" -> "text/html; charset=utf-8")
    )

  private def prettify(astNode: ASTNode): String =
    astNode match {
      case statement: Statement                => prettifier.asString(statement)
      case definition: LocalCallableDefinition => prettifier.asString(definition)
      case clause: Clause                      => prettifier.asString(SingleQuery(Seq(clause))(InputPosition.NONE))
      case groupBy: GroupBy                    => prettifier.asString(groupBy)
      case search: Search                      => prettifier.asString(search)
      case expression: Expression              => prettifier.expr(expression)
      case cypherPattern: Pattern              => prettifier.expr.patterns(cypherPattern)
      case patternPart: PatternPart            => prettifier.expr.patterns(patternPart)
      case patternElement: PatternElement      => prettifier.expr.patterns(patternElement)
      case relationship: RelationshipPattern   => prettifier.expr.patterns(relationship)
      case labelExpression: LabelExpression    => prettifier.expr.stringifyLabelExpression(labelExpression)
      case conditionalBranch @ ConditionalQueryBranch(Some(_), _) =>
        prettifier.asString(ConditionalQueryWhen(Seq(conditionalBranch), None)(InputPosition.NONE))
      case conditionalBranch @ ConditionalQueryBranch(None, _) =>
        prettifier.asString(ConditionalQueryWhen(Seq.empty, Some(conditionalBranch))(InputPosition.NONE))
      case other => other.toString
    }

  private def awaitServerReady(uri: URI): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15)
    var ready = false
    while (!ready && System.nanoTime() < deadline) {
      ready = canReach(uri)
      if (!ready) {
        Thread.sleep(150L)
      }
    }
    if (!ready) {
      throw new IllegalStateException(s"WorkingScopeInspector did not become ready at $uri")
    }
  }

  private def canReach(uri: URI): Boolean = {
    val connection = new URL(uri.toString).openConnection().asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(500)
    connection.setReadTimeout(500)
    connection.setRequestMethod("GET")
    try {
      val status = connection.getResponseCode
      status >= 200 && status < 500
    } catch {
      case _: Throwable => false
    } finally {
      connection.disconnect()
    }
  }

  private def openBrowser(uri: URI): Unit = {
    if (Desktop.isDesktopSupported) {
      val desktop = Desktop.getDesktop
      if (desktop.isSupported(Desktop.Action.BROWSE)) {
        desktop.browse(uri)
        return
      }
    }
    val osName = System.getProperty("os.name", "").toLowerCase(Locale.ROOT)
    val command =
      if (osName.contains("mac")) Seq("open", uri.toString)
      else if (osName.contains("win")) Seq("rundll32", "url.dll,FileProtocolHandler", uri.toString)
      else Seq("xdg-open", uri.toString)
    new ProcessBuilder(command: _*).start()
  }

  private val styles =
    """
      |:root {
      |  color-scheme: light;
      |  font-family: 'Public Sans', 'Nunito Sans', 'Helvetica Neue', helvetica, roboto, arial, sans-serif;
      |  background: #f5f7fa;
      |  color: #16202a;
      |}
      |
      |* {
      |  box-sizing: border-box;
      |}
      |
      |body {
      |  margin: 0;
      |  background: #f5f7fa;
      |  color: #16202a;
      |}
      |
      |.page {
      |  max-width: 1480px;
      |  margin: 0 auto;
      |  padding: 24px;
      |  display: grid;
      |  gap: 20px;
      |}
      |
      |.controls,
      |.result-panel {
      |  background: #ffffff;
      |  border: 1px solid #d7dee7;
      |  border-radius: 8px;
      |  padding: 20px;
      |  box-shadow: 0 1px 2px rgba(16, 24, 40, 0.05);
      |}
      |
      |h1,
      |h2,
      |p {
      |  margin: 0;
      |}
      |
      |.subtitle {
      |  margin-top: 6px;
      |  color: #52606d;
      |}
      |
      |.query-form {
      |  margin-top: 18px;
      |  display: grid;
      |  gap: 12px;
      |}
      |
      |.field-label,
      |.attribute-label,
      |.children-heading,
      |.children-empty,
      |.kv-key,
      |.scope-kind {
      |  font-size: 12px;
      |  text-transform: uppercase;
      |  color: #52606d;
      |}
      |
      |.field-label,
      |.attribute-label,
      |.children-heading,
      |.children-empty,
      |.scope-kind {
      |  font-weight: 700;
      |}
      |
      |.kv-key {
      |  font-weight: 300;
      |}
      |
      |textarea {
      |  width: 100%;
      |  min-height: 220px;
      |  padding: 14px;
      |  border: 1px solid #c6d0da;
      |  border-radius: 8px;
      |  resize: vertical;
      |  font: 13px/1.5 "Fira Code", SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
      |  background: #fbfcfe;
      |  color: #16202a;
      |}
      |
      |.actions {
      |  display: flex;
      |  justify-content: flex-start;
      |}
      |
      |.primary {
      |  border: 0;
      |  border-radius: 8px;
      |  padding: 10px 16px;
      |  background: #145da0;
      |  color: #ffffff;
      |  font-weight: 600;
      |  cursor: pointer;
      |}
      |
      |.primary:hover {
      |  background: #0f4f88;
      |}
      |
      |.result-panel {
      |  min-height: 180px;
      |}
      |
      |.placeholder {
      |  color: #52606d;
      |}
      |
      |.error-block {
      |  display: grid;
      |  gap: 10px;
      |  padding: 14px;
      |  border: 1px solid #efb0ab;
      |  border-radius: 8px;
      |  background: #fff4f2;
      |  color: #6e1e18;
      |  margin-bottom: 16px;
      |}
      |
      |.error-block.warning {
      |  border-color: #e6c769;
      |  background: #fff9e8;
      |  color: #6a4a00;
      |}
      |
      |.detail-block,
      |.scope-ast-preview,
      |.attribute-value.preformatted,
      |.kv-value {
      |  margin: 0;
      |  white-space: pre-wrap;
      |  word-break: break-word;
      |  font: 12px/1.55 "Fira Code", SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
      |}
      |
      |.inspection-root {
      |  display: grid;
      |  gap: 16px;
      |}
      |
      |.scope-node {
      |  display: grid;
      |  gap: 12px;
      |}
      |
      |.scope-shell {
      |  border: 1px solid #c9d5e2;
      |  border-radius: 8px;
      |  background: #fcfdff;
      |}
      |
      |.scope-summary {
      |  display: grid;
      |  grid-template-columns: minmax(0, 1fr) auto;
      |  gap: 12px;
      |  align-items: start;
      |  padding: 14px;
      |  cursor: pointer;
      |}
      |
      |.scope-summary-main {
      |  min-width: 0;
      |  display: grid;
      |  gap: 6px;
      |}
      |
      |.scope-ast-preview {
      |  color: #16202a;
      |}
      |
      |.toggle-button {
      |  width: 28px;
      |  height: 28px;
      |  border: 1px solid #c6d0da;
      |  border-radius: 6px;
      |  background: #ffffff;
      |  cursor: pointer;
      |  position: relative;
      |}
      |
      |.toggle-button::before,
      |.toggle-button::after {
      |  content: "";
      |  position: absolute;
      |  left: 50%;
      |  top: 50%;
      |  width: 12px;
      |  height: 2px;
      |  background: #2d3748;
      |  transform: translate(-50%, -50%);
      |}
      |
      |.scope-node[data-expanded="false"] > .scope-shell > .scope-summary .toggle-button::after {
      |  transform: translate(-50%, -50%) rotate(90deg);
      |}
      |
      |.scope-body {
      |  padding: 0 14px 14px;
      |  display: grid;
      |  gap: 14px;
      |}
      |
      |.scope-node[data-expanded="false"] > .scope-shell > .scope-body {
      |  display: none;
      |}
      |
      |.scope-grid {
      |  display: grid;
      |  grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
      |  gap: 12px;
      |}
      |
      |.attribute-card {
      |  border: 1px solid #d7dee7;
      |  border-radius: 8px;
      |  background: #ffffff;
      |  padding: 12px;
      |  display: grid;
      |  gap: 10px;
      |  align-content: start;
      |}
      |
      |.attribute-block {
      |  display: grid;
      |  gap: 10px;
      |}
      |
      |.kv-row {
      |  display: grid;
      |  gap: 6px;
      |}
      |
      |.variablePosition {
      |  color: #aaaaaa;
      |}
      |
      |.children-section {
      |  display: grid;
      |  gap: 12px;
      |}
      |
      |.children-list {
      |  display: grid;
      |  gap: 12px;
      |  padding-left: 16px;
      |  border-left: 2px solid #e4ebf3;
      |}
      |
      |@media (max-width: 700px) {
      |  .page {
      |    padding: 16px;
      |  }
      |
      |  .controls,
      |  .result-panel {
      |    padding: 16px;
      |  }
      |
      |  textarea {
      |    min-height: 180px;
      |  }
      |}
      |""".stripMargin

  private val scriptSource =
    """
      |const form = document.getElementById("queryForm");
      |const queryInput = document.getElementById("queryInput");
      |const result = document.getElementById("result");
      |
      |async function submitQuery(event) {
      |  if (event) event.preventDefault();
      |  const query = queryInput.value;
      |  result.innerHTML = '<div class="placeholder">Inspecting query...</div>';
      |  try {
      |    const response = await fetch(`/inspect?query=${encodeURIComponent(query)}`, {
      |      headers: { "X-Requested-With": "WorkingScopeInspector" }
      |    });
      |    const html = await response.text();
      |    result.innerHTML = html;
      |    bindScopeToggles(result);
      |    history.replaceState(null, "", `/?query=${encodeURIComponent(query)}`);
      |  } catch (error) {
      |    result.innerHTML = `<div class="error-block"><h2>Request failed</h2><pre class="detail-block">${String(error)}</pre></div>`;
      |  }
      |}
      |
      |function bindScopeToggles(root) {
      |  root.querySelectorAll('[data-toggle-scope="true"]').forEach((element) => {
      |    element.addEventListener("click", (event) => {
      |      const scopeNode = event.currentTarget.closest(".scope-node");
      |      if (!scopeNode) return;
      |      toggleScope(scopeNode);
      |    });
      |  });
      |}
      |
      |function toggleScope(scopeNode) {
      |  const expanded = scopeNode.dataset.expanded === "true";
      |  scopeNode.dataset.expanded = !expanded;
      |}
      |
      |form.addEventListener("submit", submitQuery);
      |bindScopeToggles(document);
      |""".stripMargin

  sealed private trait InspectionResult
  private case class InspectionSuccess(scope: WorkingScope, warnings: Seq[String]) extends InspectionResult
  private case class InspectionFailure(message: String, details: Seq[String]) extends InspectionResult
}
