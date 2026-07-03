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
package org.neo4j.cypher.internal.frontend.scoping.inspection_tool

import org.neo4j.cypher.internal.ast.semantics.scoping.LocalCallableScopeSignature
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionItem
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.frontend.scoping.inspection_tool.ViewModel.CallableNameListContentViewModel
import org.neo4j.cypher.internal.frontend.scoping.inspection_tool.ViewModel.CallableSignatureListContentViewModel
import org.neo4j.cypher.internal.frontend.scoping.inspection_tool.ViewModel.CardViewModel
import org.neo4j.cypher.internal.frontend.scoping.inspection_tool.ViewModel.ContentViewModel
import org.neo4j.cypher.internal.frontend.scoping.inspection_tool.ViewModel.ErrorListContentViewModel
import org.neo4j.cypher.internal.frontend.scoping.inspection_tool.ViewModel.InspectionViewModel
import org.neo4j.cypher.internal.frontend.scoping.inspection_tool.ViewModel.NestedCardContentViewModel
import org.neo4j.cypher.internal.frontend.scoping.inspection_tool.ViewModel.ProjectionItemListContentViewModel
import org.neo4j.cypher.internal.frontend.scoping.inspection_tool.ViewModel.ReferenceListContentViewModel
import org.neo4j.cypher.internal.frontend.scoping.inspection_tool.ViewModel.ScalarContentViewModel
import org.neo4j.cypher.internal.frontend.scoping.inspection_tool.ViewModel.ScopeNodeViewModel
import org.neo4j.cypher.internal.frontend.scoping.inspection_tool.ViewModel.VariableListContentViewModel
import org.neo4j.gqlstatus.ErrorGqlStatusObject

import scalatags.Text.all.*
import scalatags.Text.tags2

object Renderer {

  import Assets.scriptSource
  import Assets.styles
  import Formatting.prettifier

  private val sampleQuery =
    """MATCH (n)-[r]->(m)
      |WITH n, r, m
      |RETURN n, count(r) AS relCount""".stripMargin

  def page(query: String): String =
    "<!doctype html>" + html(
      head(
        meta(charset := "utf-8"),
        meta(name := "viewport", content := "width=device-width, initial-scale=1"),
        link(rel := "stylesheet", href := "https://fonts.googleapis.com/css?family=Public Sans"),
        link(rel := "stylesheet", href := "https://fonts.googleapis.com/css?family=Nunito Sans"),
        link(rel := "stylesheet", href := "https://fonts.googleapis.com/css?family=Helvetica Neue"),
        link(rel := "stylesheet", href := "https://fonts.googleapis.com/css?family=Fira Code"),
        tags2.title("FrontendInspector"),
        tags2.style(raw(styles))
      ),
      body(
        tags2.main(cls := "page")(
          tags2.section(cls := "controls")(
            h1("Frontend Inspector"),
            p(cls := "subtitle")("Inspect the WorkingScope tree and VariableChecker activity for a Cypher query."),
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
                button(`type` := "submit", cls := "primary")("Inspect"),
                label(cls := "toggle-control")(
                  input(id := "variableCheckerToggle", `type` := "checkbox"),
                  span(cls := "toggle-label")("Show Variable Checker log")
                )
              )
            )
          ),
          tags2.section(
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

  def renderInspection(query: String): String = {
    val trimmed = query.trim
    if (trimmed.isEmpty) {
      div(cls := "placeholder")("Enter a query to inspect.").render
    } else {
      Inspector(trimmed) match {
        case InspectionSuccess(viewModel) => renderInspectionViewModel(viewModel).render
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

  private def renderInspectionViewModel(viewModel: InspectionViewModel): Frag =
    div(
      cls := "inspection-root",
      if (viewModel.warnings.nonEmpty) div(cls := "error-block warning")(renderWarningList(viewModel.warnings)) else (),
      renderScopeNode(viewModel.root, expanded = true)
    )

  private def renderScopeNode(node: ScopeNodeViewModel, expanded: Boolean): Frag =
    div(
      cls := "scope-node",
      attr("data-expanded") := expanded.toString
    )(
      div(cls := "scope-shell")(
        div(cls := "scope-content-row")(
          div(cls := "scope-main")(
            div(cls := "scope-summary", attr("data-toggle-scope") := "true")(
              div(cls := "scope-summary-main")(
                div(cls := "scope-kind")(node.scopeKind),
                pre(cls := "scope-ast-preview")(node.astPreview)
              ),
              button(
                cls := "toggle-button",
                attr("data-target-scope") := node.id,
                `type` := "button",
                attr("aria-label") := "Toggle scope details"
              )()
            ),
            div(id := node.id, cls := "scope-body")(
              div(cls := "scope-grid")(node.details.map(renderCard))
            )
          ),
          renderVariableCheckerPanel(node)
        ),
        if (node.children.nonEmpty)
          div(cls := "children-section")(
            div(cls := "children-heading")(s"children (${node.children.size})"),
            div(cls := "children-list")(node.children.map(child => renderScopeNode(child, expanded = false)))
          )
        else
          div(cls := "children-empty")("No children")
      )
    )

  private def renderVariableCheckerPanel(node: ScopeNodeViewModel): Frag =
    div(cls := "variable-checker-panel")(
      div(cls := "variable-checker-panel-content")(
        div(cls := "variable-checker-panel-title")("Variable Checker"),
        if (node.variableCheckerEntries.nonEmpty)
          div(cls := "variable-checker-panel-entries")(node.variableCheckerEntries.map(renderVariableCheckerEntry))
        else
          div(cls := "italic-placeholder")("— no log entries —")
      )
    )

  private def renderVariableCheckerEntry(card: CardViewModel): Frag =
    div(
      cls := "attribute-card variable-checker-entry",
      attr("data-expanded") := "false"
    )(
      div(cls := "variable-checker-entry-summary", attr("data-toggle-variable-checker-entry") := "true")(
        div(cls := "attribute-label")(card.title),
        button(
          cls := "toggle-button variable-checker-entry-toggle",
          `type` := "button",
          attr("aria-label") := "Toggle variable checker entry details"
        )()
      ),
      if (card.contents.nonEmpty)
        div(cls := "variable-checker-entry-contents")(card.contents.map(renderContent))
      else
        ()
    )

  private def renderCard(card: CardViewModel): Frag =
    div(cls := "attribute-card")(
      div(cls := "attribute-label")(card.title),
      if (card.contents.nonEmpty)
        div(cls := "attribute-block")(card.contents.map(renderContent))
      else ()
    )

  private def renderContent(content: ContentViewModel): Frag = content match {
    case ScalarContentViewModel(label, value, preserveWhitespace, italic) =>
      renderScalar(label, if (italic) em(value) else frag(value), preserveWhitespace)
    case VariableListContentViewModel(label, values) =>
      renderVariableList(label, values)
    case ReferenceListContentViewModel(label, values) =>
      renderReferenceList(label, values)
    case CallableSignatureListContentViewModel(label, values) =>
      renderCallableSignatureList(label, values)
    case CallableNameListContentViewModel(values) =>
      renderCallableNameList(values)
    case ProjectionItemListContentViewModel(label, values) =>
      renderProjectionItemList(label, values)
    case ErrorListContentViewModel(values) =>
      renderErrorList(values)
    case NestedCardContentViewModel(card) =>
      renderCard(card)
  }

  private def renderScalar(label: String, value: Frag): Frag =
    renderScalar(Some(label), value, preserveWhitespace = false)

  private def renderScalar(label: String, value: Frag, preserveWhitespace: Boolean): Frag =
    renderScalar(Some(label), value, preserveWhitespace)

  private def renderScalar(label: Option[String], value: Frag, preserveWhitespace: Boolean): Frag =
    div(cls := "kv-row")(
      if (label.nonEmpty) div(cls := "kv-key")(label) else frag(),
      if (preserveWhitespace) pre(cls := "kv-value preformatted")(value) else div(cls := "kv-value")(value)
    )

  private def renderScalarWithoutLabel(value: Frag): Frag =
    renderScalar(None, value, preserveWhitespace = false)

  private def renderVariableList(label: String, values: Seq[LogicalVariable]): Frag = {
    val formatted = values.sortBy(v => (v.position.offset, v.name)).map(formatVariable)
    renderScalar(label, if (formatted.nonEmpty) joinFragsWithBreaks(formatted) else frag("-"))
  }

  private def renderReferenceList(label: String, values: Seq[(LogicalVariable, LogicalVariable)]): Frag = {
    val formatted = values.sortBy { case (reference, _) => (reference.position.offset, reference.name) }.map {
      case (reference, declaration) =>
        frag(formatVariable(reference), span(cls := "reference-arrow")("→"), formatVariable(declaration))
    }
    renderScalar(label, if (formatted.nonEmpty) joinFragsWithBreaks(formatted) else frag("-"))
  }

  private def renderCallableSignatureList(label: String, values: Seq[LocalCallableScopeSignature]): Frag = {
    val formatted = values.sortBy(_.name.fullName).map(formatCallable)
    renderScalar(label, if (formatted.nonEmpty) joinFragsWithBreaks(formatted) else frag("-"))
  }

  private def renderCallableNameList(values: Seq[org.neo4j.cypher.internal.util.CallableName]): Frag = {
    val formatted = values.sortBy(_.fullName).map(name => frag(name.fullName))
    renderScalarWithoutLabel(if (formatted.nonEmpty) joinFragsWithBreaks(formatted) else frag("-"))
  }

  private def renderProjectionItemList(label: String, values: Seq[ProjectionItem]): Frag = {
    val formatted = values.sortBy(item => (item.expression.position.offset, item.alias.map(_.name).getOrElse(""))).map {
      item => formatProjectionItem(item)
    }
    renderScalar(label, if (formatted.nonEmpty) joinFragsWithBreaks(formatted) else frag("-"))
  }

  private def renderErrorList(values: Seq[(ErrorGqlStatusObject, Int)]): Frag = {
    def getCause(gqlStatus: ErrorGqlStatusObject): ErrorGqlStatusObject = {
      if (gqlStatus.cause().isEmpty) {
        gqlStatus
      } else {
        getCause(gqlStatus.cause().get)
      }
    }

    val formatted = values.map {
      case (gqlStatus, offset) =>
        val error = getCause(gqlStatus)
        frag(
          error.getMessage,
          span(cls := "error-position")(s"@$offset")
        )
    }
    renderScalarWithoutLabel(if (formatted.nonEmpty) joinFragsWithBreaks(formatted) else frag("-"))
  }

  private def joinFragsWithBreaks(values: Seq[Frag]): Frag =
    frag(values.flatMap(value => Seq(br(), value)).drop(1))

  private def renderWarningList(warnings: Seq[String]): Frag =
    div(
      h2("Frontend reported errors"),
      pre(cls := "detail-block")(warnings.sorted.mkString("\n"))
    )

  private def formatVariable(variable: LogicalVariable): Frag =
    frag(
      s"${variable.name}",
      span(cls := "variable-position")(s"@${variable.position.offset}")
    )

  private def formatCallable(callable: LocalCallableScopeSignature): Frag =
    frag(s"${callable.name.fullName} : ${formatResult(callable.result)}")

  private def formatProjectionItem(item: ProjectionItem): Frag =
    frag(
      prettifier.expr(item.expression),
      item.alias.map(alias => frag(" AS ", formatVariable(alias))).getOrElse(frag())
    )

  private def formatResult(result: org.neo4j.cypher.internal.ast.semantics.scoping.Result): String =
    result match {
      case org.neo4j.cypher.internal.ast.semantics.scoping.TableResult(columns) =>
        s"${result.getClass.getSimpleName}(${columns.iterator.map(formatVariable).mkString(", ")})"
      case other =>
        other.getClass.getSimpleName
    }
}
