/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.diagnostics;

import java.util.List;
import java.util.Map;

/**
 * The materialized result of a query or procedure executed through a {@link DiagnosticsLiveConnection}.
 * <p>
 * Deliberately expressed in terms of plain Java types so that the diagnostics abstraction does not leak the
 * underlying connection technology (e.g. the Neo4j driver) to its implementors.
 *
 * @param columns the column names, in result order.
 * @param rows one map per record, each keyed by column name. Iteration order of a row matches {@code columns}.
 */
public record DiagnosticsQueryResult(List<String> columns, List<Map<String, Object>> rows) {}
