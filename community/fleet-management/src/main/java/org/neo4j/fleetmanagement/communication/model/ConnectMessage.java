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

@JsonClassDescription("Message sent from the server to the Fleet Manager service to establish a connection.")
public class ConnectMessage {
    @JsonProperty("server_id")
    @JsonPropertyDescription("Unique identifier for the server")
    public String serverId;

    @JsonPropertyDescription("Name of the server")
    public String name;

    @JsonProperty("dbms_id")
    @JsonPropertyDescription("Unique identifier for the DBMS")
    public String dbmsId;

    @JsonProperty("server_version")
    @JsonPropertyDescription("Version of the server")
    public String serverVersion;

    @JsonProperty("project_id")
    @JsonPropertyDescription("Identifier for the project")
    public String projectId;

    @JsonProperty("plugin_version")
    @JsonPropertyDescription("Version of the Fleet Manager module")
    public String pluginVersion;

    @JsonProperty("build_profile")
    @JsonPropertyDescription("Build profile of the Fleet Manager module")
    public String buildProfile;

    @JsonProperty("is_system_db_writer")
    @JsonPropertyDescription("Whether this server allows writing to system DB")
    public boolean isSystemDbWriter;

    public ConnectMessage(
            String serverId,
            String name,
            String dbmsId,
            String serverVersion,
            String projectId,
            String pluginVersion,
            boolean isSystemDbWriter) {
        this.serverId = serverId;
        this.name = name;
        this.dbmsId = dbmsId;
        this.serverVersion = serverVersion;
        this.projectId = projectId;
        this.pluginVersion = pluginVersion;
        this.buildProfile = "embedded";
        this.isSystemDbWriter = isSystemDbWriter;
    }
}
