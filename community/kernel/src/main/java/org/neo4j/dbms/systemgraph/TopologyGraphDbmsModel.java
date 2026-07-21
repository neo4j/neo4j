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
package org.neo4j.dbms.systemgraph;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.cypher.internal.CypherVersion;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.NormalizedCatalogEntry;
import org.neo4j.kernel.database.NormalizedDatabaseName;

public interface TopologyGraphDbmsModel extends TopologyGraphDbmsModelConstants {
    enum HostedOnMode {
        RAFT(1, "raft"),
        REPLICA(2, "replica"),
        SINGLE(0, "single"),
        VIRTUAL(3, "virtual");

        private final String modeName;
        private final byte code;

        HostedOnMode(int code, String modeName) {
            this.code = (byte) code;
            this.modeName = modeName;
        }

        public String modeName() {
            return modeName;
        }

        public byte code() {
            return code;
        }

        public static HostedOnMode from(String modeName) {
            requireNonNull(modeName);

            for (HostedOnMode mode : values()) {
                if (modeName.equals(mode.modeName)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("Enum value not found for requested modeName: " + modeName);
        }

        public static HostedOnMode forCode(byte code) {
            return Arrays.stream(values())
                    .filter(value -> value.code == code)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Invalid hosted on mode: " + code));
        }
    }

    enum DatabaseStatus {
        ONLINE("online"),
        OFFLINE("offline");

        private final String statusName;

        DatabaseStatus(String statusName) {
            this.statusName = statusName;
        }

        public String statusName() {
            return statusName;
        }

        public static DatabaseStatus fromName(String statusName) {
            return Arrays.stream(values())
                    .filter(databaseStatus -> databaseStatus.statusName().equals(statusName))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(format("No such status '%s'", statusName)));
        }
    }

    enum DatabaseAccess {
        READ_ONLY(0, "read-only"),
        READ_WRITE(1, "read-write");

        private final String stringRepr;
        private final byte code;

        DatabaseAccess(int code, String stringRepr) {
            this.code = (byte) code;
            this.stringRepr = stringRepr;
        }

        public static DatabaseAccess forCode(byte code) {
            return Stream.of(values())
                    .filter(v -> v.getCode() == code)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Can't find database access with code " + code));
        }

        public String getStringRepr() {
            return stringRepr;
        }

        public byte getCode() {
            return code;
        }

        public static DatabaseAccess toDatabaseAccess(boolean readOnly) {
            if (values().length != 2) {
                throw new IllegalStateException("Can't identify database access");
            }
            return readOnly ? DatabaseAccess.READ_ONLY : DatabaseAccess.READ_WRITE;
        }

        public static DatabaseAccess create(Object value) {
            Objects.requireNonNull(value);

            var access = value.toString().toUpperCase(Locale.ROOT);
            try {
                return DatabaseAccess.valueOf(access);
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("Unsupported database access: " + access, ex);
            }
        }
    }

    enum InstanceStatus {
        ENABLED,
        DEALLOCATING,
        CORDONED;

        public static InstanceStatus getInstanceStatus(String value) {
            if (value.equals("active")) {
                return InstanceStatus.ENABLED;
            } else if (value.equals("deallocating")) {
                return InstanceStatus.DEALLOCATING;
            }
            return InstanceStatus.valueOf(value);
        }
    }

    /**
     * Fetches all known database references
     */
    Set<DatabaseReference> getAllDatabaseReferences();

    /**
     * Fetches all known composite database references
     */
    Set<DatabaseReferenceImpl.Composite> getAllCompositeDatabaseReferences();

    /**
     * Fetches the {@link NamedDatabaseId} corresponding to the provided alias, if one exists in this DBMS.
     * <p>
     * Note: The returned id will have its *true* name (primary alias), rather than the provided databaseName, which may be a (secondary) alias.
     *
     * @param databaseName the database alias to resolve a {@link NamedDatabaseId} for.
     * @return the corresponding {@link NamedDatabaseId}
     */
    Optional<NamedDatabaseId> getDatabaseIdByAlias(String databaseName);

    /**
     * Fetches the {@link NamedDatabaseId} corresponding to the provided id, if one exists in this DBMS.
     *
     * @param uuid the uuid to resolve a {@link NamedDatabaseId} for.
     * @return the corresponding {@link NamedDatabaseId}
     */
    Optional<NamedDatabaseId> getDatabaseIdByUUID(UUID uuid);

    Optional<NamedDatabaseId> getDatabaseIdByUUID(UUID uuid, boolean resolveToShardedDb);

    /**
     * Fetches the {@link DatabaseReference} corresponding to the provided catalog entry.
     *
     * @param catalogEntry the catalog entry to resolve a {@link DatabaseReference} for.
     * @return the corresponding {@link DatabaseReference}
     */
    Optional<DatabaseReference> getDatabaseRefByAlias(NormalizedCatalogEntry catalogEntry);

    /**
     * Fetches the {@link DatabaseReference} corresponding to the provided alias.
     * The alias can be a database name, a local alias, a remote alias, or any alias within a composite namespace.
     *
     * @param alias the catalog entry to resolve a {@link DatabaseReference} for.
     * @return the corresponding {@link DatabaseReference}
     */
    default Optional<DatabaseReference> getDatabaseRefByAlias(String alias) {
        return getDatabaseRefByAlias(new NormalizedCatalogEntry(alias)).or(() -> {
            var parts = alias.split("\\.");
            // database is maybe in a different namespace so try that
            for (int i = 1; i < parts.length; i++) {
                var namespace = Stream.of(parts).limit(i).collect(Collectors.joining("."));
                var name = Stream.of(parts).skip(i).collect(Collectors.joining("."));
                var result = getDatabaseRefByAlias(new NormalizedCatalogEntry(namespace, name));
                if (result.isPresent()) {
                    return result;
                }
            }
            return Optional.empty();
        });
    }

    /**
     * Fetches the {@link DatabaseReference} corresponding to the provided database name display name.
     *
     * @param name the display name of the {@link DatabaseReference} to be resolved.
     * @return the corresponding {@link DatabaseReference}
     */
    Optional<DatabaseReference> getDatabaseRefByDisplayName(NormalizedDatabaseName name);

    /**
     * Fetches the {@link DriverSettings} corresponding to the provided database name
     * if the name exists and is associated with a {@link DatabaseReferenceImpl.External}
     *
     * @param databaseName - the remote database alias to resolve driver settings for
     * @param namespace    - the namespace of the remote database alias to resolve driver settings for
     * @return the corresponding {@link DriverSettings}
     */
    Optional<DriverSettings> getDriverSettings(String databaseName, String namespace);

    Optional<Map<String, Object>> getAliasProperties(String databaseName, String namespace);

    /**
     * Returns the default query language of a remote alias.
     * This default query language is only needed for remote aliases in the default namespace.
     * For remote aliases with a composite namespace, the default query language is sourced from the composite database.
     * @param remoteAliasName in default namespace
     * @return defined default query language of the given remote alias
     */
    Optional<CypherVersion> getRemoteAliasLanguageVersion(String remoteAliasName);

    /**
     * Fetches the {@link ExternalDatabaseCredentials} corresponding to the provided database name
     * if the name exists and is associated with a {@link DatabaseReferenceImpl.External}
     *
     * @param databaseReference - the remote database reference to resolve driver settings for
     * @return the corresponding {@link ExternalDatabaseCredentials}
     */
    Optional<ExternalDatabaseCredentials> getExternalDatabaseCredentials(
            DatabaseReferenceImpl.External databaseReference);
}
