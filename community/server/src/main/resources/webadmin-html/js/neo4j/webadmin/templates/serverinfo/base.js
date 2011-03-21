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
define(function(){return function(vars){ with(vars||{}) { return "<div class=\"sidebar\"><ol>" + 
(function () { var __result__ = [], __key__, domain; for (__key__ in domains) { if (domains.hasOwnProperty(__key__)) { domain = domains[__key__]; __result__.push(
"<lh>" + 
domain.name + 
"</lh>" +
(function () { var __result__ = [], __key__, bean; for (__key__ in domain.beans) { if (domain.beans.hasOwnProperty(__key__)) { bean = domain.beans[__key__]; __result__.push(
"<li><a href=\"#/info/" +
bean.domain +
"/" +
bean.getName() +
"/\">" + 
bean.getName() + 
"</a></li>"
); } } return __result__.join(""); }).call(this)
); } } return __result__.join(""); }).call(this) + 
"</ol></div><div class=\"workarea with-sidebar\" id=\"info-bean\"></div>";}}; });