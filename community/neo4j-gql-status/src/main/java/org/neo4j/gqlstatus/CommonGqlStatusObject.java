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
package org.neo4j.gqlstatus;

import java.util.Map;
import org.neo4j.annotations.api.PublicApi;

/**
 * Representation for a GQL-status object.
 * A GQL-status object can be visualized in a client to show diagnostic information and the status of the execution.
 */
@PublicApi
public interface CommonGqlStatusObject {

    /**
     * Returns a GQLSTATUS code representing the status of the query execution.
     * @return the GQLSTATUS code
     */
    String gqlStatus();

    /**
     * Returns a longer description of the GQLSTATUS code.
     * @return the status description
     */
    String statusDescription();

    /**
     * Returns diagnostic information associated with the GQLSTATUS code.
     * @return the diagnostic record
     */
    Map<String, Object> diagnosticRecord();
}
