/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphdb;

/**
 * The string search mode is used together with a value template to find nodes of interest.
 * The search mode can be one of:
 * <ul>
 *   <li>EXACT: The value has to match the template exactly.</li>
 *   <li>PREFIX: The value must have a prefix matching the template.</li>
 *   <li>SUFFIX: The value must have a suffix matching the template.</li>
 *   <li>CONTAINS: The value must contain the template. Only exact matches are supported.</li>
 * </ul>
 */
public enum StringSearchMode
{
    /**
     * The value has to match the template exactly.
     */
    EXACT,
    /**
     * The value must have a prefix matching the template.
     */
    PREFIX,
    /**
     * The value must have a suffix matching the template.
     */
    SUFFIX,
    /**
     * The value must contain the template exactly. Regular expressions are not supported.
     */
    CONTAINS;
}
