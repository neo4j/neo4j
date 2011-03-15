/*
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
define(function(){return function(vars){ with(vars||{}) { return "<div class=\"workarea pad\"><div class=\"controls\"><input value=\"" +
query +
"\" id=\"data-console\" /><button title=\"Search\" class=\"icon-button\" id=\"data-execute-console\"></button><div class=\"data-toolbar\"><button title=\"Create a node\" class=\"text-icon-button\" id=\"data-create-node\">Node</button><button title=\"Create a relationship\" class=\"text-icon-button\" id=\"data-create-relationship\">Relationship</button>" +
(function () { if (viewType === "tabular"      ) { return (
"<button title=\"Switch view mode\" class=\"icon-button\" id=\"data-switch-view\"></button>"
);} else { return ""; } }).call(this) +
(function () { if (viewType !== "tabular"      ) { return (
"<button title=\"Switch view mode\" class=\"icon-button tabular\" id=\"data-switch-view\"></button>"
);} else { return ""; } }).call(this) +
"<button title=\"Go to reference node\" class=\"icon-button\" id=\"data-home\"></button><button title=\"Refresh current data\" class=\"icon-button\" id=\"data-refresh\"></button></div><div class=\"break\"></div></div><div id=\"data-area\"></div></div>";}}; });