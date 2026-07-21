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

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

public interface TopologyGraphDbmsModelConstants {
    Label DATABASE_LABEL = Label.label("Database");
    String DATABASE = DATABASE_LABEL.name();
    Label DELETED_DATABASE_LABEL = Label.label("DeletedDatabase");
    String DATABASE_UUID_PROPERTY = "uuid";
    String DATABASE_NAME_PROPERTY = "name";
    String DATABASE_STATUS_PROPERTY = "status";
    String DATABASE_ACCESS_PROPERTY = "access";

    @Deprecated
    String DATABASE_DEFAULT_PROPERTY = "default";

    @Deprecated
    String DATABASE_VIRTUAL_PROPERTY = "virtual";

    String DATABASE_UPDATE_ID_PROPERTY = "update_id";
    String DATABASE_STORE_RANDOM_ID_PROPERTY = "store_random_id";

    @Deprecated
    String DATABASE_DESIGNATED_SEEDER_PROPERTY = "designated_seeder";

    String DATABASE_SEEDING_SERVERS_PROPERTY = "seeding_servers";
    String DATABASE_STORE_FORMAT_NEW_DB_PROPERTY = "creation_store_format";
    String DATABASE_STORE_MANIPULATION_PROPERTY = "store_manipulation";
    String DATABASE_PRIMARIES_PROPERTY = "primaries";
    String DATABASE_SECONDARIES_PROPERTY = "secondaries";
    String DATABASE_SEED_URI_PROPERTY = "seedURI";
    String DATABASE_SEED_SOURCE_DATABASE_PROPERTY = "seedSourceDatabase";
    String DATABASE_SEED_CREDENTIALS_ENCRYPTED_PROPERTY = "seedCredentialsEncrypted";
    String DATABASE_SEED_CREDENTIALS_IV_PROPERTY = "seedCredentialsIv";
    String DATABASE_SEED_CONFIG_PROPERTY = "seedConfig";
    String DATABASE_SEED_RESTORE_UNTIL_PROPERTY = "seedRestoreUntil";
    String DATABASE_CREATED_AT_PROPERTY = "created_at";
    String DATABASE_STARTED_AT_PROPERTY = "started_at";
    String DATABASE_UPDATED_AT_PROPERTY = "updated_at";
    String DATABASE_STOPPED_AT_PROPERTY = "stopped_at";
    String DATABASE_EXTERNAL_ID_PROPERTY = "external_id";
    String DELETED_DATABASE_DUMP_DATA_PROPERTY = "dump_data";
    String DELETED_DATABASE_DELETED_AT_PROPERTY = "deleted_at";
    String DELETED_DATABASE_KEEP_DATA_PROPERTY = "keep_data";
    String DATABASE_LOG_ENRICHMENT_PROPERTY = "txLogEnrichment";
    String DATABASE_BOOTSTRAP_KERNEL_VERSION_PROPERTY = "bootstrapKernelVersion";
    String DATABASE_DEFAULT_LANGUAGE_PROPERTY = "defaultLanguage";
    String DATABASE_BACKPRESSURE_ENABLED_PROPERTY = "backpressureEnabled";
    String DATABASE_MIRROR_DESCRIPTOR_PROPERTY = "mirrorDescriptor";
    Label DATABASE_NAME_LABEL = Label.label("DatabaseName");
    String DATABASE_NAME = DATABASE_NAME_LABEL.name();
    String DATABASE_NAME_LABEL_DESCRIPTION = "Database alias";
    Label SPD_LABEL = Label.label("Spd");
    String SPD = SPD_LABEL.name();
    Label GRAPH_SHARD_LABEL = Label.label("GraphShard");
    String GRAPH_SHARD = GRAPH_SHARD_LABEL.name();
    Label PROPERTY_SHARD_LABEL = Label.label("PropertyShard");
    String PROPERTY_SHARD = PROPERTY_SHARD_LABEL.name();
    Label COMPOSITE_DATABASE_LABEL = Label.label("CompositeDatabase");
    String COMPOSITE_DATABASE = COMPOSITE_DATABASE_LABEL.name();
    Label GRAPH_ENGINE_DATABASE_LABEL = Label.label("GraphEngine");
    String NAME_PROPERTY = "name";
    String VERSION_PROPERTY = "version"; // used to refresh connection pool on change
    RelationshipType TARGETS_RELATIONSHIP = RelationshipType.withName("TARGETS");
    String TARGETS = TARGETS_RELATIONSHIP.name();
    String TARGET_NAME_PROPERTY = "target_name";
    String PRIMARY_PROPERTY = "primary";
    String NAMESPACE_PROPERTY = "namespace";
    String DISPLAY_NAME_PROPERTY = "displayName";
    String DISPLAY_NAME_CONSTRAINT = "displayNameConstraint";
    String QUOTED_DISPLAY_NAME_PROPERTY = "quotedDisplayName";
    String DEFAULT_NAMESPACE = "system-root";
    Label REMOTE_DATABASE_LABEL = Label.label("Remote");
    String REMOTE_DATABASE = REMOTE_DATABASE_LABEL.name();
    String REMOTE_DATABASE_LABEL_DESCRIPTION = "Remote Database alias";
    String URL_PROPERTY = "url";
    String REMOTE_USERNAME_PROPERTY = "username";
    String REMOTE_PASSWORD_PROPERTY = "password";
    String OIDC_CREDENTIAL_FORWARDING_PROPERTY = "oidc_credential_forwarding";
    String IV_PROPERTY = "iv"; // Initialization Vector for AES encryption
    Label DRIVER_SETTINGS_LABEL = Label.label("DriverSettings");
    String DRIVER_SETTINGS = DRIVER_SETTINGS_LABEL.name();
    String SSL_ENFORCED = "ssl_enforced";
    String CONNECTION_TIMEOUT = "connection_timeout";
    String CONNECTION_MAX_LIFETIME = "connection_max_lifetime";
    String CONNECTION_POOL_ACQUISITION_TIMEOUT = "connection_pool_acquisition_timeout";
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
    String HOSTED_ON_INITIAL_PROPERTY =
            "bootstrapper"; // the key and the value mismatch here, because of backward compatibility
    String HOSTED_ON_RAFT_MEMBER_ID_PROPERTY = "raftMemberId";
    String HOSTED_ON_MODE_PROPERTY = "mode";
    String WAS_HOSTED_ON_REMOVED_AT_PROPERTY = "removed_at";
    String WAS_HOSTED_ON_INITIAL_PROPERTY =
            "was_bootstrapper"; // the key and the value mismatch here, because of backward compatibility
    Label TOPOLOGY_GRAPH_CONFIG_LABEL = Label.label("TopologyGraphSettings");

    @Deprecated
    String TOPOLOGY_GRAPH_CONFIG_ALLOCATOR_PROPERTY = "allocator";

    String TOPOLOGY_GRAPH_CONFIG_DEFAULT_NUMBER_OF_PRIMARIES_PROPERTY = "default_number_of_primaries";
    String TOPOLOGY_GRAPH_CONFIG_DEFAULT_NUMBER_OF_SECONDARIES_PROPERTY = "default_number_of_secondaries";
    String TOPOLOGY_GRAPH_CONFIG_DEFAULT_NUMBER_OF_PROPERTY_SHARD_REPLICAS_PROPERTY =
            "default_number_of_property_shard_replicas";
    String TOPOLOGY_GRAPH_CONFIG_DEFAULT_DATABASE_PROPERTY = "default_database";
    String TOPOLOGY_GRAPH_CONFIG_DEFAULT_DATABASE_CREATE_ATTEMPTED_FLAG = "default_database_created";
    String TOPOLOGY_GRAPH_CONFIG_INITIAL_INSTANCES_ENABLED_FLAG = "initial_instances_enabled";
    String TOPOLOGY_GRAPH_CONFIG_AUTO_ENABLE_FREE_SERVERS_FLAG = "auto_enable_free_servers";
    String TOPOLOGY_GRAPH_CONFIG_CREATE_DEFAULT_DATABASE = "create_default_database";
    String TOPOLOGY_GRAPH_CONFIG_DEFAULT_ALLOCATION_HINT_PREFIX = "default_allocation_hint_";
    Label SUPPORTED_COMPONENT_VERSIONS_LABEL = Label.label("SupportedVersions");
    String SUPPORTED_COMPONENT_VERSIONS_UUID_PROPERTY = "__uuid";
    RelationshipType LATEST_SUPPORTED_COMPONENT_VERSIONS_RELATIONSHIP =
            RelationshipType.withName("LATEST_SUPPORTED_VERSIONS");
    String HAS_PROPERTY_SHARD = "HAS_PROPERTY_SHARD";
    RelationshipType HAS_PROPERTY_SHARD_RELATIONSHIP = RelationshipType.withName(HAS_PROPERTY_SHARD);
    String HAS_GRAPH_SHARD = "HAS_GRAPH_SHARD";
    RelationshipType HAS_GRAPH_SHARD_RELATIONSHIP = RelationshipType.withName(HAS_GRAPH_SHARD);
    String HAS_PROPERTY_SHARD_INDEX_PROPERTY = "index";
    Label ALLOCATION_HINTS_LABEL = Label.label("AllocationHints");
    RelationshipType HAS_ALLOCATION_HINTS_RELATIONSHIP = RelationshipType.withName("HAS_ALLOCATION_HINTS");
    Label MIRROR_LABEL = Label.label("Mirror");
}
