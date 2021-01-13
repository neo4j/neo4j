/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.GraphDatabaseSettings.LogQueryLevel;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.configuration.ssl.SslPolicyScope;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.logging.Log;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.consistency_checker_fail_fast_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.default_advertised_address;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.default_listen_address;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_database_max_size;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_max_size;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_warmup_prefetch_allowlist;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_allowlist;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_database_default;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_max_off_heap_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_off_heap_block_cache_size;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_off_heap_max_cacheable_block_size;
import static org.neo4j.configuration.SettingValueParsers.LIST_SEPARATOR;
import static org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS;

public final class SettingMigrators
{
    private SettingMigrators()
    {
    }

    @ServiceProvider
    public static class ActiveDatabaseMigrator implements SettingMigrator
    {
        @Override
        public void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log )
        {
            migrateSettingNameChange( values, log, "dbms.active_database", default_database );
        }
    }

    @ServiceProvider
    public static class DefaultSchemaProviderMigrator implements SettingMigrator
    {
        private static final String INDEX_PROVIDER_SETTING = "dbms.index.default_schema_provider";

        @Override
        public void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log )
        {
            String oldSchemaProvider = values.remove( INDEX_PROVIDER_SETTING );
            if ( isBlank( oldSchemaProvider ) )
            {
                return;
            }
            String newSchemaProvider = oldSchemaProvider;
            boolean migratedValue = false;
            switch ( oldSchemaProvider )
            {
            case "lucene-1.0":
            case "lucene+native-1.0":
                newSchemaProvider = "lucene+native-3.0";
                migratedValue = true;
                break;
            case "lucene+native-2.0":
                newSchemaProvider = "native-btree-1.0";
                migratedValue = true;
                break;
            default:
            }

            String warning = "Use of deprecated setting " + INDEX_PROVIDER_SETTING + ".";
            if ( migratedValue )
            {
                warning += " Value migrated from " + oldSchemaProvider + " to " + newSchemaProvider + ".";
            }
            log.warn( warning );
            values.put( INDEX_PROVIDER_SETTING, newSchemaProvider );
        }
    }

    @ServiceProvider
    public static class CrsConfigMigrator implements SettingMigrator
    {
        private static final String PREFIX = "unsupported.dbms.db.spatial.crs";
        private static final Pattern oldConnector = Pattern.compile( "^unsupported\\.dbms\\.db\\.spatial\\.crs\\.([^.]+)\\.(min|max)\\.([xyz])$");

        @Override
        public void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log )
        {
            List<String> oldCrs = new ArrayList<>();
            Map<String, List<String>> crsValues = new HashMap<>();
            values.forEach( ( setting, value ) ->
            {
                Matcher matcher = oldConnector.matcher( setting );
                if ( matcher.find() )
                {
                    String crsName = matcher.group( 1 );
                    String crsPlusSetting = format( "%s.%s", crsName, matcher.group( 2 ) );
                    CoordinateReferenceSystem crs = CoordinateReferenceSystem.byName( crsName );
                    List<String> valueList = crsValues.computeIfAbsent( crsPlusSetting,
                            s -> new ArrayList<>( Collections.nCopies( crs.getDimension(), Double.toString( Double.NaN ) ) ) );
                    valueList.set( matcher.group( 3 ).charAt( 0 ) - 'x' , value );
                    oldCrs.add( setting );
                }
            } );

            oldCrs.forEach( setting -> {
                values.remove( setting );
                log.warn( "Use of deprecated setting %s.", setting );
            } );
            crsValues.forEach( ( name, valueList ) -> {
                String setting = format( "%s.%s", PREFIX, name );
                String value = String.join( LIST_SEPARATOR, valueList );
                values.putIfAbsent( setting, value );
                log.warn( "Settings migrated to %s = %s", setting, value );
            } );
        }
    }

    @ServiceProvider
    public static class ConnectorMigrator implements SettingMigrator
    {
        private static final Pattern oldConnector = Pattern.compile( "^dbms\\.connector\\.([^.]+)\\.([^.]+)$");
        private static final String ANY_CONNECTOR = "bolt|http|https";

        @Override
        public void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log )
        {
            migrateOldConnectors( values, log );
            migrateConnectorAddresses( values, defaultValues, log );
        }

        private static void migrateOldConnectors( Map<String,String> values, Log log )
        {
            Map<String, Matcher> oldConnectors = new HashMap<>();
            values.forEach( ( setting, value ) ->
            {
                Matcher matcher = oldConnector.matcher( setting );
                if ( matcher.find() )
                {
                    oldConnectors.put( setting, matcher );
                }
            } );

            oldConnectors.forEach( ( setting, matcher ) -> {
                String settingName = matcher.group( 2 );
                String id = matcher.group( 1 );
                if ( id.matches( ANY_CONNECTOR ) )
                {
                    if ( Objects.equals( "type", settingName ) )
                    {
                        values.remove( setting );
                        log.warn( "Use of deprecated setting %s. Type is no longer required", setting );
                    }
                }
                else
                {
                    values.remove( setting );
                    log.warn( "Use of deprecated setting %s. No longer supports multiple connectors. Setting discarded.", setting );
                }
            } );
        }

        private static void migrateConnectorAddresses( Map<String,String> values, Map<String,String> defValues,  Log log )
        {
            migrateAdvertisedAddressInheritanceChange( values, defValues, log, BoltConnector.listen_address.name(), BoltConnector.advertised_address.name() );
            migrateAdvertisedAddressInheritanceChange( values, defValues, log, HttpConnector.listen_address.name(), HttpConnector.advertised_address.name() );
            migrateAdvertisedAddressInheritanceChange( values, defValues, log, HttpsConnector.listen_address.name(), HttpsConnector.advertised_address.name() );
        }
    }

    @ServiceProvider
    public static class DefaultAddressMigrator implements SettingMigrator
    {
        @Override
        public void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log )
        {
            migrateSettingNameChange( values, log, "dbms.connectors.default_listen_address", default_listen_address );
            migrateSettingNameChange( values, log, "dbms.connectors.default_advertised_address", default_advertised_address );
        }
    }

    @ServiceProvider
    public static class SslPolicyMigrator implements SettingMigrator
    {
        private static final Pattern pattern = Pattern.compile( "^(dbms\\.ssl\\.policy\\.)([^.]+)(\\.[^.]+)$" );
        private static final Map<String,SslPolicyScope> settingScopeMap = Map.of(
                "bolt.ssl_policy", SslPolicyScope.BOLT,
                "https.ssl_policy", SslPolicyScope.HTTPS,
                "dbms.backup.ssl_policy", SslPolicyScope.BACKUP,
                "causal_clustering.ssl_policy", SslPolicyScope.CLUSTER
        );

        private static final List<String> legacySettings =
                List.of( "dbms.directories.certificates", "unsupported.dbms.security.tls_certificate_file", "unsupported.dbms.security.tls_key_file" );

        @Override
        public void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log )
        {
            migratePolicies( values, log );
            warnUseOfLegacyPolicy( values, log );
        }

        private static void migratePolicies( Map<String,String> values, Log log )
        {
            Map<String,String> valueCopy = new HashMap<>( values );
            Map<String,SslPolicyScope> oldNameToScope = new HashMap<>();
            valueCopy.forEach( ( setting, value ) -> {
                if ( settingScopeMap.containsKey( setting ) )
                {
                    log.warn( "Use of deprecated setting %s.", setting );
                    SslPolicyScope scope = settingScopeMap.get( setting );
                    oldNameToScope.put( value, scope );
                    values.put( SslPolicyConfig.forScope( scope ).enabled.name(), Boolean.TRUE.toString() );
                    values.remove( setting );
                }
            } );

            valueCopy.forEach( ( setting, value ) -> {
                var matcher = pattern.matcher( setting );
                if ( matcher.find() )
                {
                    String groupName = matcher.group( 2 );
                    if ( oldNameToScope.containsKey( groupName ) )
                    {
                        String newGroupName = oldNameToScope.get( groupName ).name().toLowerCase();
                        if ( !Objects.equals( groupName, newGroupName ) )
                        {
                            String prefix = matcher.group( 1 );
                            String suffix = matcher.group( 3 );
                            String newSetting = prefix + newGroupName + suffix;

                            log.warn( "Use of deprecated setting %s. It is replaced by %s", setting, newSetting );
                            values.remove( setting );
                            values.put( newSetting, value );
                        }
                    }
                }
            } );
        }

        private static void warnUseOfLegacyPolicy( Map<String,String> values, Log log )
        {
            for ( String legacySetting : legacySettings )
            {
                if ( values.remove( legacySetting ) != null )
                {
                    log.warn( "Use of deprecated setting %s. Legacy ssl policy is no longer supported.", legacySetting );
                }
            }
        }
    }

    @ServiceProvider
    public static class AllowKeyGenerationMigrator implements SettingMigrator
    {
        private static final Pattern pattern = Pattern.compile( "^dbms\\.ssl\\.policy\\.([^.]+)\\.allow_key_generation$" );

        @Override
        public void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log )
        {
            var toRemove = new HashSet<String>();

            for ( var setting : values.keySet() )
            {
                var matcher = pattern.matcher( setting );
                if ( matcher.find() )
                {
                    log.warn( "Setting %s is removed. A valid key and certificate are required" +
                            " to be present in the key and certificate path configured in this ssl policy.", setting );
                    toRemove.add( setting );
                }
            }

            values.keySet().removeAll( toRemove );
        }
    }

    @ServiceProvider
    public static class KillQueryVerboseMigrator implements SettingMigrator
    {
        private static final String settingName = "dbms.procedures.kill_query_verbose";

        @Override
        public void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log )
        {
            migrateSettingRemoval( values, log, settingName, "It's no longer possible to disable verbose kill query logging" );
        }
    }

    @ServiceProvider
    public static class MultiThreadedSchemaIndexPopulationEnabledMigrator implements SettingMigrator
    {
        private static final String settingName = "unsupported.dbms.multi_threaded_schema_index_population_enabled";

        @Override
        public void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log )
        {
            migrateSettingRemoval( values, log, settingName, "It's no longer possible to disable multi-threaded index population" );
        }
    }

    @ServiceProvider
    public static class QueryLoggerMigrator implements SettingMigrator
    {
        private static final String deprecationMessage = "Use of deprecated setting value %s=%s. It is replaced by %s=%s";
        private static final String settingName = GraphDatabaseSettings.log_queries.name();

        @Override
        public void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log )
        {
            String value = values.get( settingName );
            if ( SettingValueParsers.TRUE.equalsIgnoreCase( value ) )
            {
                log.warn( deprecationMessage, settingName, value, settingName, LogQueryLevel.INFO.name() );
                values.put( settingName, LogQueryLevel.INFO.name() );
            }
            else if ( SettingValueParsers.FALSE.equalsIgnoreCase( value ) )
            {
                log.warn( deprecationMessage, settingName, value, settingName, LogQueryLevel.OFF.name() );
                values.put( settingName, LogQueryLevel.OFF.name() );
            }
        }
    }

    @ServiceProvider
    public static class DatabaseMemoryMigrator implements SettingMigrator
    {
        @Override
        public void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log )
        {
            migrateSettingNameChange( values, log, "dbms.tx_state.max_off_heap_memory", tx_state_max_off_heap_memory );
            migrateSettingNameChange( values, log, "dbms.tx_state.off_heap.max_cacheable_block_size", tx_state_off_heap_max_cacheable_block_size );
            migrateSettingNameChange( values, log, "dbms.tx_state.off_heap.block_cache_size", tx_state_off_heap_block_cache_size );

            // Migrate cypher.query_max_allocations to new setting, if new settings is not configured
            String maxAllocations = values.remove( "cypher.query_max_allocations" );
            if ( isNotBlank( maxAllocations ) )
            {
                if ( !values.containsKey( memory_transaction_max_size.name() ) )
                {
                    log.warn( "The setting cypher.query_max_allocations is removed and replaced by %s.", memory_transaction_max_size.name() );
                    values.put( memory_transaction_max_size.name(), maxAllocations );
                }
                else
                {
                    log.warn( "The setting cypher.query_max_allocations is removed and replaced by %s. Since both are set, %s will take " +
                                    "precedence and the value of cypher.query_max_allocations, %s, will be ignored.",
                            memory_transaction_max_size.name(), memory_transaction_max_size.name(), maxAllocations );
                }
            }
        }
    }

    @ServiceProvider
    public static class WhitelistSettingsMigrator implements SettingMigrator
    {
        @Override
        public void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log )
        {
            migrateSettingNameChange( values, log, "dbms.memory.pagecache.warmup.preload.whitelist", pagecache_warmup_prefetch_allowlist );
            migrateSettingNameChange( values, log, "dbms.security.procedures.whitelist", procedure_allowlist );
        }
    }

    /**
     * Fix typo in setting name: datababase -> database
     */
    @ServiceProvider
    public static class DatababaseMigrator implements SettingMigrator
    {
        @Override
        public void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log )
        {
            migrateSettingNameChange( values, log, "dbms.memory.transaction.datababase_max_size", memory_transaction_database_max_size );
        }
    }

    @ServiceProvider
    public static class RefuseToBeLeaderMigration implements SettingMigrator
    {

        @Override
        public void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log )
        {
            final var refuseToBeLeader = "causal_clustering.refuse_to_be_leader";
            final var refuseToBeLeaderValue = values.get( refuseToBeLeader );
            if ( isNotBlank( refuseToBeLeaderValue ) )
            {
                log.warn( "The setting " + refuseToBeLeader + " is deprecated. Use please %s as a replacement", read_only_database_default.name() );
            }
            migrateSettingNameChange( values, log, refuseToBeLeader, read_only_database_default );
        }
    }

    public static void migrateAdvertisedAddressInheritanceChange( Map<String,String> values, Map<String,String> defaultValues,
            Log log, String listenAddress, String advertisedAddress )
    {
        String listenValue = values.get( listenAddress );
        if ( isNotBlank( listenValue ) )
        {
            String advertisedValue = values.get( advertisedAddress );
            boolean advertisedAlreadyHasPort = false;
            try
            {
                if ( isNotBlank( advertisedValue ) )
                {
                    advertisedAlreadyHasPort = SOCKET_ADDRESS.parse( advertisedValue ).getPort() >= 0;
                }
            }
            catch ( RuntimeException e )
            {
                // If we cant parse the advertised address we act as if it has no port specified
                // If invalid hostname config will report the error
            }

            if ( !advertisedAlreadyHasPort )
            {
                try
                {
                    int port = SOCKET_ADDRESS.parse( listenValue ).getPort();
                    if ( port >= 0 ) //valid port on listen, and none on advertised, migrate!
                    {
                        SocketAddress newAdvertised = new SocketAddress( advertisedValue, port );
                        String msg = "Note that since you did not explicitly set the port in %s Neo4j automatically set it to %s to match %s." +
                                " This behavior may change in the future and we recommend you to explicitly set it.";
                        if ( isNotBlank( advertisedValue ) )
                        {
                            //If advertised has an address set (not inherited or default value we treat this as a warning, since is is likely to be used.
                            log.warn( msg, advertisedAddress, port, listenAddress );
                        }
                        else
                        {
                            //No value was set, likely the user does not care or won't use this. Just provide the info
                            log.info( msg, advertisedAddress, port, listenAddress );
                        }
                        defaultValues.put( advertisedAddress, newAdvertised.toString() );
                    }
                }
                catch ( RuntimeException e )
                {
                    //If we cant parse the listen address we have no information on how to proceed with the migration
                    // The config will handle the error later
                }
            }
        }
    }

    @ServiceProvider
    public static class ConsistencyCheckerSettingsMigrator implements SettingMigrator
    {
        @Override
        public void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log )
        {
            migrateSettingRemoval( values, log, "unsupported.consistency_checker.experimental",
                    "There is no longer multiple different consistency checkers to choose from" );
            migrateSettingNameChange( values, log, "unsupported.consistency_checker.experimental.fail_fast", consistency_checker_fail_fast_threshold );
        }
    }

    public static void migrateSettingNameChange( Map<String,String> values, Log log, String oldSetting, Setting<?> newSetting )
    {
        String value = values.remove( oldSetting );
        if ( isNotBlank( value ) )
        {
            log.warn( "Use of deprecated setting %s. It is replaced by %s", oldSetting, newSetting.name() );
            values.putIfAbsent( newSetting.name(), value );
        }
    }

    public static void migrateSettingRemoval( Map<String,String> values, Log log, String name, String additionalDescription )
    {
        if ( values.containsKey( name ) )
        {
            log.warn( "Setting %s is removed. %s.", name, additionalDescription );
            values.remove( name );
        }
    }
}
