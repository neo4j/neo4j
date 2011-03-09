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
define(function(){return function(vars){ with(vars||{}) { return (function () { if (typeof(bean) != "undefined") { return (
"<div class=\"pad\"><h2>" + 
bean.getName() + 
"</h2><p class=\"small\">" + 
bean.description + 
"</p></div><table cellspacing=\"0\" class=\"data-table\"><tbody>" + 
(function () { var __result__ = [], __key__, attribute; for (__key__ in attributes) { if (attributes.hasOwnProperty(__key__)) { attribute = attributes[__key__]; __result__.push(
"<tr><td style=\"padding-left:" +
attribute.indent*10 +
"px;\"><h3>" + 
attribute.name + 
"</h3><p class=\"small\">" + 
attribute.description + 
"</p></td><td class=\"value\">" + 
attribute.value + 
"</td></tr>"
); } } return __result__.join(""); }).call(this) + 
"</tbody></table>"
);} else { return ""; } }).call(this) +
(function () { if (typeof(bean) == "undefined") { return (
"<h2>No bean found</h2>"
);} else { return ""; } }).call(this);}}; });