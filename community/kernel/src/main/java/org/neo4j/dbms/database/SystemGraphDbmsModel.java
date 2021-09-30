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
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;

public class SystemGraphDbmsModel
{
    public enum HostedOnMode
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

    public enum DatabaseStatus
    {
        online,offline
    }

    public enum DatabaseAccess
    {
        READ_ONLY, READ_WRITE
    }

    public enum InstanceStatus
    {
        active,draining
    }

    public static final Label DATABASE_LABEL = Label.label( "Database" );
    public static final Label DELETED_DATABASE_LABEL = Label.label( "DeletedDatabase" );
    public static final String DATABASE_UUID_PROPERTY = "uuid";
    public static final String DATABASE_NAME_PROPERTY = "name";
    public static final String DATABASE_STATUS_PROPERTY = "status";
    public static final String DATABASE_ACCESS_PROPERTY = "access";
    public static final String DATABASE_DEFAULT_PROPERTY = "default";
    public static final String DATABASE_UPDATE_ID_PROPERTY = "update_id";
    public static final String DATABASE_INITIAL_SERVERS_PROPERTY = "initial_members";
    public static final String DATABASE_STORE_CREATION_TIME_PROPERTY = "store_creation_time";
    public static final String DATABASE_STORE_RANDOM_ID_PROPERTY = "store_random_id";
    public static final String DATABASE_STORE_VERSION_PROPERTY = "store_version";
    public static final String DATABASE_DESIGNATED_SEEDER_PROPERTY = "designated_seeder";
    public static final String DATABASE_STORAGE_ENGINE_PROPERTY = "storage_engine";
    public static final String DATABASE_PRIMARIES_PROPERTY = "primaries";
    public static final String DATABASE_SECONDARIES_PROPERTY = "secondaries";
    public static final String DATABASE_CREATED_AT_PROPERTY = "created_at";
    public static final String DATABASE_STARTED_AT_PROPERTY = "started_at";
    public static final String DATABASE_UPDATED_AT_PROPERTY = "updated_at";
    public static final String DATABASE_STOPPED_AT_PROPERTY = "stopped_at";
    public static final String DELETED_DATABASE_DUMP_DATA_PROPERTY = "dump_data";
    public static final String DELETED_DATABASE_DELETED_AT_PROPERTY = "deleted_at";

    public static final Label INSTANCE_LABEL = Label.label( "Instance" );
    public static final Label REMOVED_INSTANCE_LABEL = Label.label( "RemovedInstance" );
    public static final String INSTANCE_UUID_PROPERTY = "uuid";
    public static final String INSTANCE_STATUS_PROPERTY = "status";
    public static final String INSTANCE_DISCOVERED_AT_PROPERTY = "discovered_at";
    public static final String INSTANCE_MODE_PROPERTY = "mode";
    public static final String REMOVED_INSTANCE_REMOVED_AT_PROPERTY = "removed_at";
    public static final String REMOVED_INSTANCE_ALIASES_PROPERTY = "aliases";

    public static final RelationshipType HOSTED_ON_RELATIONSHIP = RelationshipType.withName( "HOSTED_ON" );
    public static final RelationshipType WAS_HOSTED_ON_RELATIONSHIP = RelationshipType.withName( "WAS_HOSTED_ON" );
    public static final String HOSTED_ON_INSTALLED_AT_PROPERTY = "installed_at";
    public static final String HOSTED_ON_BOOTSTRAPPER_PROPERTY = "bootstrapper";
    public static final String HOSTED_ON_MODE_PROPERTY = "mode";
    public static final String WAS_HOSTED_ON_REMOVED_AT_PROPERTY = "removed_at";

    public static final Label TOPOLOGY_GRAPH_SETTINGS_LABEL = Label.label( "TopologyGraphSettings" );
    public static final String TOPOLOGY_GRAPH_SETTINGS_ALLOCATOR_PROPERTY = "allocator";

    private final Transaction tx;

    public SystemGraphDbmsModel( Transaction tx )
    {
        this.tx = tx;
    }

    public Map<NamedDatabaseId,DatabaseAccess> getAllDatabaseAccess()
    {
        return tx.findNodes( DATABASE_LABEL ).stream()
                 .collect( Collectors.toMap( SystemGraphDbmsModel::getDatabaseId, SystemGraphDbmsModel::getDatabaseAccess ) );
    }

    private static NamedDatabaseId getDatabaseId( Node databaseNode )
    {
        var name = (String) databaseNode.getProperty( DATABASE_NAME_PROPERTY );
        var uuid = UUID.fromString( (String) databaseNode.getProperty( DATABASE_UUID_PROPERTY ) );
        return DatabaseIdFactory.from( name, uuid );
    }

    private static DatabaseAccess getDatabaseAccess( Node databaseNode )
    {
        var accessString = (String) databaseNode.getProperty( DATABASE_ACCESS_PROPERTY, DatabaseAccess.READ_WRITE.toString() );
        return Enum.valueOf( DatabaseAccess.class, accessString );
    }
}
