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
define(function(){return function(vars){ with(vars||{}) { return "<p><label for=\"create-relationship-from\">From node</label><input type=\"text\" value=\"" +
from +
"\" id=\"create-relationship-from\" /></p><p><label for=\"create-relationship-type\">Type</label><input type=\"text\" value=\"" +
type +
"\" id=\"create-relationship-type\" /></p><p><label for=\"create-relationship-types\"></label><select id=\"create-relationship-types\">&nbsp;<option>Types in use</option>" +
(function () { var __result__ = [], __key__, type; for (__key__ in types) { if (types.hasOwnProperty(__key__)) { type = types[__key__]; __result__.push(
"<option>" + 
type + 
"</option>"
); } } return __result__.join(""); }).call(this) + 
"</select></p><p><label for=\"create-relationship-to\">To node</label><input type=\"text\" value=\"" +
to +
"\" id=\"create-relationship-to\" /></p><p><button id=\"create-relationship\">Create</button></p>";}}; });