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

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.database.NamedDatabaseId;

public interface TopologyGraphDbmsModel {
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
                    .orElseThrow(() -> new IllegalArgumentException(String.format("No such status '%s'", statusName)));
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

    Label DATABASE_LABEL = Label.label("Database");
    String DATABASE = DATABASE_LABEL.name();
    Label DELETED_DATABASE_LABEL = Label.label("DeletedDatabase");
    String DATABASE_UUID_PROPERTY = "uuid";
    String DATABASE_NAME_PROPERTY = "name";
    String DATABASE_STATUS_PROPERTY = "status";
    String DATABASE_ACCESS_PROPERTY = "access";
    String DATABASE_DEFAULT_PROPERTY = "default";
    String DATABASE_VIRTUAL_PROPERTY = "virtual";
    String DATABASE_UPDATE_ID_PROPERTY = "update_id";
    String DATABASE_STORE_RANDOM_ID_PROPERTY = "store_random_id";

    @Deprecated
    String DATABASE_DESIGNATED_SEEDER_PROPERTY = "designated_seeder";

    String DATABASE_SEEDING_SERVERS_PROPERTY = "seeding_servers";
    String DATABASE_STORE_FORMAT_NEW_DB_PROPERTY = "creation_store_format";
    String DATABASE_PRIMARIES_PROPERTY = "primaries";
    String DATABASE_SECONDARIES_PROPERTY = "secondaries";
    String DATABASE_SEED_URI_PROPERTY = "seedURI";
    String DATABASE_SEED_CREDENTIALS_ENCRYPTED_PROPERTY = "seedCredentialsEncrypted";
    String DATABASE_SEED_CREDENTIALS_IV_PROPERTY = "seedCredentialsIv";
    String DATABASE_SEED_CONFIG_PROPERTY = "seedConfig";
    String DATABASE_CREATED_AT_PROPERTY = "created_at";
    String DATABASE_STARTED_AT_PROPERTY = "started_at";
    String DATABASE_UPDATED_AT_PROPERTY = "updated_at";
    String DATABASE_STOPPED_AT_PROPERTY = "stopped_at";
    String DELETED_DATABASE_DUMP_DATA_PROPERTY = "dump_data";
    String DELETED_DATABASE_DELETED_AT_PROPERTY = "deleted_at";
    String DELETED_DATABASE_KEEP_DATA_PROPERTY = "keep_data";
    String DATABASE_LOG_ENRICHMENT_PROPERTY = "txLogEnrichment";
    String DATABASE_BOOTSTRAP_KERNEL_VERSION_PROPERTY = "bootstrapKernelVersion";

    Label DATABASE_NAME_LABEL = Label.label("DatabaseName");
    String DATABASE_NAME = DATABASE_NAME_LABEL.name();
    String DATABASE_NAME_LABEL_DESCRIPTION = "Database alias";

    Label COMPOSITE_DATABASE_LABEL = Label.label("CompositeDatabase");
    String COMPOSITE_DATABASE = COMPOSITE_DATABASE_LABEL.name();
    String NAME_PROPERTY = "name";
    String VERSION_PROPERTY = "version"; // used to refresh connection pool on change
    RelationshipType TARGETS_RELATIONSHIP = RelationshipType.withName("TARGETS");
    String TARGETS = TARGETS_RELATIONSHIP.name();
    String TARGET_NAME_PROPERTY = "target_name";
    String PRIMARY_PROPERTY = "primary";
    String NAMESPACE_PROPERTY = "namespace";

    String DISPLAY_NAME_PROPERTY = "displayName";
    String DEFAULT_NAMESPACE = "system-root";
    Label REMOTE_DATABASE_LABEL = Label.label("Remote");
    String REMOTE_DATABASE = REMOTE_DATABASE_LABEL.name();
    String REMOTE_DATABASE_LABEL_DESCRIPTION = "Remote Database alias";
    String URL_PROPERTY = "url";
    String USERNAME_PROPERTY = "username";
    String PASSWORD_PROPERTY = "password";
    String IV_PROPERTY = "iv"; // Initialization Vector for AES encryption
    Label DRIVER_SETTINGS_LABEL = Label.label("DriverSettings");
    String DRIVER_SETTINGS = DRIVER_SETTINGS_LABEL.name();
    String SSL_ENFORCED = "ssl_enforced";
    String CONNECTION_TIMEOUT = "connection_timeout";
    String CONNECTION_MAX_LIFETIME = "connection_max_lifetime";
    String CONNECTION_POOL_AQUISITION_TIMEOUT = "connection_pool_acquisition_timeout";
    String CONNECTION_POOL_IDLE_TEST = "connection_pool_idle_test";
    String CONNECTION_POOL_MAX_SIZE = "connection_pool_max_size";
    String LOGGING_LEVEL = "logging_level";

    RelationshipType CONNECTS_WITH_RELATIONSHIP = RelationshipType.withName("CONNECTS_WITH");
    String CONNECTS_WITH = CONNECTS_WITH_RELATIONSHIP.name();

    RelationshipType PROPERTIES_RELATIONSHIP = RelationshipType.withName("PROPERTIES");
    String PROPERTIES = PROPERTIES_RELATIONSHIP.name();
    Label ALIAS_PROPERTIES_LABEL = Label.label("AliasProperties");
    String ALIAS_PROPERTIES = ALIAS_PROPERTIES_LABEL.name();
    Label INSTANCE_LABEL = Label.label("Instance");
    Label REMOVED_INSTANCE_LABEL = Label.label("RemovedInstance");
    String INSTANCE_UUID_PROPERTY = "uuid";
    String INSTANCE_NAME_PROPERTY = "name";
    String INSTANCE_STATUS_PROPERTY = "status";
    String INSTANCE_DISCOVERED_AT_PROPERTY = "discovered_at";

    String INSTANCE_ALLOWED_DATABASES_PROPERTY = "allowedDatabases";
    String INSTANCE_DENIED_DATABASES_PROPERTY = "deniedDatabases";
    String INSTANCE_MODE_CONSTRAINT_PROPERTY = "modeConstraint";
    String INSTANCE_TAGS_PROPERTY = "tags";
    String REMOVED_INSTANCE_REMOVED_AT_PROPERTY = "removed_at";

    RelationshipType HOSTED_ON_RELATIONSHIP = RelationshipType.withName("HOSTED_ON");
    RelationshipType WAS_HOSTED_ON_RELATIONSHIP = RelationshipType.withName("WAS_HOSTED_ON");
    String HOSTED_ON_INSTALLED_AT_PROPERTY = "installed_at";
    String HOSTED_ON_BOOTSTRAPPER_PROPERTY = "bootstrapper";
    String HOSTED_ON_RAFT_MEMBER_ID_PROPERTY = "raftMemberId";
    String HOSTED_ON_MODE_PROPERTY = "mode";
    String WAS_HOSTED_ON_REMOVED_AT_PROPERTY = "removed_at";
    String WAS_HOSTED_ON_BOOTSTRAPPER_PROPERTY = "was_bootstrapper";
    Label TOPOLOGY_GRAPH_CONFIG_LABEL = Label.label("TopologyGraphSettings");

    @Deprecated
    String TOPOLOGY_GRAPH_CONFIG_ALLOCATOR_PROPERTY = "allocator";

    String TOPOLOGY_GRAPH_CONFIG_DEFAULT_NUMBER_OF_PRIMARIES_PROPERTY = "default_number_of_primaries";
    String TOPOLOGY_GRAPH_CONFIG_DEFAULT_NUMBER_OF_SECONDARIES_PROPERTY = "default_number_of_secondaries";
    String TOPOLOGY_GRAPH_CONFIG_DEFAULT_DATABASE_PROPERTY = "default_database";
    String TOPOLOGY_GRAPH_CONFIG_DEFAULT_DATABASE_CREATE_ATTEMPTED_FLAG = "default_database_created";
    String TOPOLOGY_GRAPH_CONFIG_INITIAL_INSTANCES_ENABLED_FLAG = "initial_instances_enabled";
    String TOPOLOGY_GRAPH_CONFIG_AUTO_ENABLE_FREE_SERVERS_FLAG = "auto_enable_free_servers";
    String TOPOLOGY_GRAPH_CREATE_DEFAULT_DATABASE = "create_default_database";
    Label SUPPORTED_COMPONENT_VERSIONS_LABEL = Label.label("SupportedVersions");
    String SUPPORTED_COMPONENT_VERSIONS_UUID_PROPERTY = "__uuid";
    RelationshipType LATEST_SUPPORTED_COMPONENT_VERSIONS_RELATIONSHIP =
            RelationshipType.withName("LATEST_SUPPORTED_VERSIONS");

    RelationshipType HAS_SHARD = RelationshipType.withName("HAS_SHARD");
    String HAS_SHARD_INDEX_PROPERTY = "index";

    /**
     * Fetches the {@link NamedDatabaseId} corresponding to the provided alias, if one exists in this DBMS.
     * <p>
     * Note: The returned id will have its *true* name (primary alias), rather than the provided databaseName, which may be an (secondary) alias.
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

    /**
     * Fetches all known database references
     */
    Set<DatabaseReference> getAllDatabaseReferences();

    /**
     * Fetches all known internal database references
     */
    Set<DatabaseReferenceImpl.Internal> getAllInternalDatabaseReferences();

    /**
     * Fetches all known external database references
     */
    Set<DatabaseReferenceImpl.External> getAllExternalDatabaseReferences();

    /**
     * Fetches all known composite database references
     */
    Set<DatabaseReferenceImpl.Composite> getAllCompositeDatabaseReferences();

    /**
     * Fetches all known sharded property database references
     */
    Set<DatabaseReferenceImpl.SPD> getAllShardedPropertyDatabaseReferences();

    /**
     * Fetches the {@link DatabaseReference} corresponding to the provided name.
     *
     * @param databaseName the database alias to resolve a {@link DatabaseReference} for.
     * @return the corresponding {@link DatabaseReference}
     */
    Optional<DatabaseReference> getDatabaseRefByAlias(String databaseName);

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
     * Fetches the {@link ExternalDatabaseCredentials} corresponding to the provided database name
     * if the name exists and is associated with a {@link DatabaseReferenceImpl.External}
     *
     * @param databaseReference - the remote database reference to resolve driver settings for
     * @return the corresponding {@link ExternalDatabaseCredentials}
     */
    Optional<ExternalDatabaseCredentials> getExternalDatabaseCredentials(
            DatabaseReferenceImpl.External databaseReference);

    static String readOwningDatabase(Node aliasNode) {
        var relationships = aliasNode.getRelationships(Direction.INCOMING, TopologyGraphDbmsModel.HAS_SHARD).stream()
                .map(Relationship::getStartNode)
                .toList(); // exhaust cursor
        if (relationships.isEmpty()) {
            return aliasNode.getProperty(DATABASE_NAME_PROPERTY).toString();
        } else {
            return relationships.get(0).getProperty(DATABASE_NAME_PROPERTY).toString();
        }
    }
}
