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
package org.neo4j.fleetmanagement.communication.model;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

@JsonClassDescription(
        "Message sent from the server to the Fleet Manager service to retrieve details of a migration to Aura.")
public class MigrationTokenMessage {
    @JsonProperty("project_id")
    @JsonPropertyDescription("Identifier for the project")
    public String projectId;

    @JsonProperty("dbms_id")
    @JsonPropertyDescription("Unique identifier for the DBMS")
    public String dbmsId;

    @JsonProperty("server_id")
    @JsonPropertyDescription("Unique identifier for the server")
    public String serverId;

    @JsonProperty("migration_id")
    @JsonPropertyDescription("ID of the migration")
    public String migrationId;
}
