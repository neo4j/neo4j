/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.dbms.database;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.database.NamedDatabaseId;

public interface TopologyGraphDbmsModel
{
    enum HostedOnMode
    {
        raft( GraphDatabaseSettings.Mode.CORE ),
        replica( GraphDatabaseSettings.Mode.READ_REPLICA ),
        single( GraphDatabaseSettings.Mode.SINGLE );

        private final GraphDatabaseSettings.Mode instanceMode;

        HostedOnMode( GraphDatabaseSettings.Mode instanceMode )
        {
            this.instanceMode = instanceMode;
        }

        public GraphDatabaseSettings.Mode instanceMode()
        {
            return instanceMode;
        }

        public static HostedOnMode from( GraphDatabaseSettings.Mode instanceMode )
        {
            for ( HostedOnMode mode : values() )
            {
                if ( mode.instanceMode == instanceMode )
                {
                    return mode;
                }
            }
            throw new IllegalArgumentException( "Invalid instance mode found: " + instanceMode );
        }
    }

    enum DatabaseStatus
    {
        online,offline
    }

    enum DatabaseAccess
    {
        READ_ONLY( "read-only" ), READ_WRITE( "read-write" );

        private final String stringRepr;

        DatabaseAccess( String stringRepr )
        {
            this.stringRepr = stringRepr;
        }

        public String getStringRepr()
        {
            return stringRepr;
        }
    }

    enum InstanceStatus
    {
        active,draining
    }

    Label DATABASE_LABEL = Label.label( "Database" );
    Label DELETED_DATABASE_LABEL = Label.label( "DeletedDatabase" );
    String DATABASE_UUID_PROPERTY = "uuid";
    String DATABASE_NAME_PROPERTY = "name";
    String DATABASE_STATUS_PROPERTY = "status";
    String DATABASE_ACCESS_PROPERTY = "access";
    String DATABASE_DEFAULT_PROPERTY = "default";
    String DATABASE_UPDATE_ID_PROPERTY = "update_id";
    String DATABASE_INITIAL_SERVERS_PROPERTY = "initial_members";
    String DATABASE_STORE_CREATION_TIME_PROPERTY = "store_creation_time";
    String DATABASE_STORE_RANDOM_ID_PROPERTY = "store_random_id";
    String DATABASE_STORE_VERSION_PROPERTY = "store_version";
    String DATABASE_DESIGNATED_SEEDER_PROPERTY = "designated_seeder";
    String DATABASE_STORAGE_ENGINE_PROPERTY = "storage_engine";
    String DATABASE_PRIMARIES_PROPERTY = "primaries";
    String DATABASE_SECONDARIES_PROPERTY = "secondaries";
    String DATABASE_CREATED_AT_PROPERTY = "created_at";
    String DATABASE_STARTED_AT_PROPERTY = "started_at";
    String DATABASE_UPDATED_AT_PROPERTY = "updated_at";
    String DATABASE_STOPPED_AT_PROPERTY = "stopped_at";
    String DELETED_DATABASE_DUMP_DATA_PROPERTY = "dump_data";
    String DELETED_DATABASE_DELETED_AT_PROPERTY = "deleted_at";

    Label INSTANCE_LABEL = Label.label( "Instance" );
    Label REMOVED_INSTANCE_LABEL = Label.label( "RemovedInstance" );
    String INSTANCE_UUID_PROPERTY = "uuid";
    String INSTANCE_STATUS_PROPERTY = "status";
    String INSTANCE_DISCOVERED_AT_PROPERTY = "discovered_at";
    String INSTANCE_MODE_PROPERTY = "mode";
    String REMOVED_INSTANCE_REMOVED_AT_PROPERTY = "removed_at";
    String REMOVED_INSTANCE_ALIASES_PROPERTY = "aliases";

    RelationshipType HOSTED_ON_RELATIONSHIP = RelationshipType.withName( "HOSTED_ON" );
    RelationshipType WAS_HOSTED_ON_RELATIONSHIP = RelationshipType.withName( "WAS_HOSTED_ON" );
    String HOSTED_ON_INSTALLED_AT_PROPERTY = "installed_at";
    String HOSTED_ON_BOOTSTRAPPER_PROPERTY = "bootstrapper";
    String HOSTED_ON_MODE_PROPERTY = "mode";
    String WAS_HOSTED_ON_REMOVED_AT_PROPERTY = "removed_at";

    Label TOPOLOGY_GRAPH_SETTINGS_LABEL = Label.label( "TopologyGraphSettings" );
    String TOPOLOGY_GRAPH_SETTINGS_ALLOCATOR_PROPERTY = "allocator";

    Label DATABASE_ALIAS_LABEL = Label.label( "DatabaseAlias" );
    RelationshipType TARGETS_DATABASE_RELATIONSHIP = RelationshipType.withName( "TARGETS_DATABASE" );

    Set<NamedDatabaseId> getAllDatabaseIds();
    Optional<NamedDatabaseId> getDatabaseIdByAlias( String databaseName );
    Optional<NamedDatabaseId> getDatabaseIdByUUID( UUID uuid );
    Map<String,NamedDatabaseId> getAllDatabaseAliases();
}
