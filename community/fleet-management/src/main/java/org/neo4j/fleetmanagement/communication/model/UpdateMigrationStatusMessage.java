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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.time.Instant;
import org.apache.commons.lang3.exception.ExceptionUtils;

@JsonClassDescription(
        "Message sent from the server to the Fleet Manager service to update migration status during a migration to Aura.")
public class UpdateMigrationStatusMessage {
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

    @JsonProperty("migration_step")
    @JsonPropertyDescription("Name of the migration step to update status for")
    public MigrationStep migrationStep;

    @JsonProperty("started_at")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyDescription("Timestamp when the migration step started")
    public Instant startedAt;

    @JsonProperty("completed_at")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyDescription("Timestamp when the migration step completed")
    public Instant completedAt;

    @JsonProperty("logs")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyDescription("Logs from the migration step, if any")
    public MigrationStepLogs logs;

    @JsonProperty("error")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyDescription("Error details from the migration step, if any")
    public MigrationStepError error;

    public enum MigrationStep {
        @JsonProperty("dump")
        DUMP,
        @JsonProperty("upload")
        UPLOAD,
        @JsonProperty("import")
        IMPORT
    }

    public static class MigrationStepLogs {
        @JsonProperty("stdout")
        @JsonPropertyDescription("Standard output from the migration step")
        public String stdOut;

        @JsonProperty("stderr")
        @JsonPropertyDescription("Standard error output from the migration step")
        public String stdErr;

        public MigrationStepLogs(String stdOut, String stdErr) {
            this.stdOut = stdOut;
            this.stdErr = stdErr;
        }
    }

    public static class MigrationStepError {
        @JsonProperty("message")
        @JsonPropertyDescription("Error message")
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String message;

        @JsonProperty("stacktrace")
        @JsonPropertyDescription("Stacktrace")
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String stacktrace;

        public MigrationStepError(Throwable exception) {
            this.message = exception.getMessage();
            this.stacktrace = ExceptionUtils.getStackTrace(exception);
        }
    }
}
