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
define(function(){return function(vars){ with(vars||{}) { return "<ul>" + 
(function () { var __result__ = [], __key__, line; for (__key__ in lines) { if (lines.hasOwnProperty(__key__)) { line = lines[__key__]; __result__.push(
"<li>" + 
line + 
"</li>"
); } } return __result__.join(""); }).call(this) +
"<li>" + 
(function () { if (showPrompt) { return (
"gremlin> <input type=\"text\" value=\"" +
prompt +
"\" id=\"console-input\" />"
);} else { return ""; } }).call(this) + 
"</li></ul>";}}; });