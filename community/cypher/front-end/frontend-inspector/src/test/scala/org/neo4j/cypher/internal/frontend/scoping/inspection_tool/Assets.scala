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

object Assets {

  val styles: String =
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
      |  max-width: 1840px;
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
      |.scope-kind,
      |.variable-checker-panel-title,
      |.toggle-label {
      |  font-size: 12px;
      |  text-transform: uppercase;
      |  color: #52606d;
      |}
      |
      |.field-label,
      |.attribute-label,
      |.children-heading,
      |.children-empty,
      |.scope-kind,
      |.variable-checker-panel-title,
      |.toggle-label {
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
      |  align-items: center;
      |  gap: 16px;
      |  flex-wrap: wrap;
      |}
      |
      |.toggle-control {
      |  display: inline-flex;
      |  align-items: center;
      |  gap: 10px;
      |}
      |
      |.toggle-control input {
      |  margin: 0;
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
      |.italic-placeholder {
      |  color: #52606d;
      |  font-style: italic;
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
      |  display: grid;
      |  border: 1px solid #c9d5e2;
      |  border-radius: 8px;
      |  background: #fcfdff;
      |  overflow: hidden;
      |}
      |
      |.scope-content-row {
      |  display: grid;
      |  grid-template-columns: minmax(0, 1fr) 0;
      |}
      |
      |.scope-node[data-expanded="true"] > .scope-shell:has(> .children-section) > .scope-content-row {
      |  border-bottom: 1px solid #f3f8fd; /*#d7dee7;*/
      |}
      |
      |body[data-show-variable-checker="true"] .scope-content-row {
      |  grid-template-columns: minmax(0, 1fr) minmax(320px, 380px);
      |}
      |
      |.scope-main {
      |  min-width: 0;
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
      |.scope-node[data-expanded="false"] > .scope-shell > .scope-content-row > .scope-main > .scope-summary .toggle-button::after {
      |  transform: translate(-50%, -50%) rotate(90deg);
      |}
      |
      |.scope-body {
      |  padding: 0 14px 14px;
      |  display: grid;
      |  gap: 14px;
      |}
      |
      |.scope-node[data-expanded="false"] > .scope-shell > .scope-content-row > .scope-main > .scope-body {
      |  display: none;
      |}
      |
      |.scope-node[data-expanded="false"] > .scope-shell > .children-section,
      |.scope-node[data-expanded="false"] > .scope-shell > .children-empty {
      |  display: none;
      |}
      |
      |.scope-grid,
      |.variable-checker-entry-contents {
      |  display: grid;
      |  grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
      |  gap: 12px;
      |}
      |
      |.variable-checker-entry-summary {
      |  display: grid;
      |  grid-template-columns: minmax(0, 1fr) auto;
      |  gap: 12px;
      |  align-items: start;
      |  cursor: pointer;
      |}
      |
      |.variable-checker-entry-toggle {
      |  width: 24px;
      |  height: 24px;
      |}
      |
      |.variable-checker-entry[data-expanded="false"] > .variable-checker-entry-summary .variable-checker-entry-toggle::after {
      |  transform: translate(-50%, -50%) rotate(90deg);
      |}
      |
      |.variable-checker-entry[data-expanded="false"] > .variable-checker-entry-contents {
      |  display: none;
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
      |.attribute-block,
      |.variable-checker-panel-content,
      |.variable-checker-panel-entries {
      |  display: grid;
      |  gap: 10px;
      |}
      |
      |.kv-row {
      |  display: grid;
      |  gap: 6px;
      |}
      |
      |.variable-position {
      |  margin-left: 1em;
      |  color: #aaaaaa;
      |}
      |
      |.reference-arrow {
      |  margin: 0 0.6em;
      |  color: #888888;
      |}
      |
      |.error-position {
      |  margin-left: 1em;
      |  color: #aaaaaa;
      |}
      |
      |.children-section {
      |  display: grid;
      |  gap: 12px;
      |  padding: 14px 14px 14px;
      |}
      |
      |.children-empty {
      |  padding: 0 14px 14px;
      |}
      |
      |.children-list {
      |  display: grid;
      |  gap: 12px;
      |  padding-left: 16px;
      |  border-left: 2px solid #e4ebf3;
      |}
      |
      |.variable-checker-panel {
      |  display: none;
      |  min-width: 0;
      |  padding: 14px;
      |  border-left: 1px solid #f3f8fd; /*#d7dee7;*/
      |  background: linear-gradient(180deg, #f8fbff 0%, #f3f8fd 100%);
      |}
      |
      |body[data-show-variable-checker="true"] .variable-checker-panel {
      |  display: block;
      |}
      |
      |.scope-node[data-expanded="false"] > .scope-shell > .scope-content-row > .variable-checker-panel > .variable-checker-panel-content {
      |  display: none;
      |}
      |
      |@media (max-width: 1200px) {
      |  body[data-show-variable-checker="true"] .scope-content-row {
      |    grid-template-columns: minmax(0, 1fr);
      |  }
      |
      |  body[data-show-variable-checker="true"] .variable-checker-panel {
      |    border-left: 0;
      |    border-top: 1px solid #f3f8fd; /*#d7dee7;*/
      |  }
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

  val scriptSource: String =
    """
      |const form = document.getElementById("queryForm");
      |const queryInput = document.getElementById("queryInput");
      |const result = document.getElementById("result");
      |const variableCheckerToggle = document.getElementById("variableCheckerToggle");
      |
      |function updateVariableCheckerState() {
      |  document.body.dataset.showVariableChecker = variableCheckerToggle.checked ? "true" : "false";
      |}
      |
      |async function submitQuery(event) {
      |  if (event) event.preventDefault();
      |  const query = queryInput.value;
      |  result.innerHTML = '<div class="placeholder">Inspecting query...</div>';
      |  try {
      |    const response = await fetch(`/inspect?query=${encodeURIComponent(query)}`, {
      |      headers: { "X-Requested-With": "FrontendInspector" }
      |    });
      |    const html = await response.text();
      |    result.innerHTML = html;
      |    bindScopeToggles(result);
      |    history.replaceState(null, "", `/?query=${encodeURIComponent(query)}`);
      |    updateVariableCheckerState();
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
      |  root.querySelectorAll('[data-toggle-variable-checker-entry="true"]').forEach((element) => {
      |    element.addEventListener("click", (event) => {
      |      const entryNode = event.currentTarget.closest(".variable-checker-entry");
      |      if (!entryNode) return;
      |      toggleVariableCheckerEntry(entryNode);
      |    });
      |  });
      |}
      |
      |function toggleScope(scopeNode) {
      |  const expanded = scopeNode.dataset.expanded === "true";
      |  scopeNode.dataset.expanded = (!expanded).toString();
      |}
      |
      |function toggleVariableCheckerEntry(entryNode) {
      |  const expanded = entryNode.dataset.expanded === "true";
      |  entryNode.dataset.expanded = (!expanded).toString();
      |}
      |
      |form.addEventListener("submit", submitQuery);
      |variableCheckerToggle.addEventListener("change", updateVariableCheckerState);
      |bindScopeToggles(document);
      |updateVariableCheckerState();
      |""".stripMargin
}
