/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel;

import static java.util.Arrays.asList;
import static org.neo4j.backup.OnlineBackupExtension.parsePort;
import static org.neo4j.kernel.Config.ENABLE_ONLINE_BACKUP;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.neo4j.com.Client;
import org.neo4j.com.Server;
import org.neo4j.kernel.ha.AsyncZooKeeperLastCommittedTxIdSetter;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.TimeUtil;
import org.neo4j.kernel.ha.ZooKeeperLastCommittedTxIdSetter;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.core.LastCommittedTxIdSetter;

public class HaConfig
{
    public static final String CONFIG_KEY_OLD_SERVER_ID = "ha.machine_id";
    public static final String CONFIG_KEY_SERVER_ID = "ha.server_id";
    public static final String CONFIG_KEY_OLD_COORDINATORS = "ha.zoo_keeper_servers";
    public static final String CONFIG_KEY_COORDINATORS = "ha.coordinators";
    public static final String CONFIG_KEY_SERVER = "ha.server";
    public static final String CONFIG_KEY_CLUSTER_NAME = "ha.cluster_name";
    public static final String CONFIG_KEY_PULL_INTERVAL = "ha.pull_interval";
    public static final String CONFIG_KEY_ALLOW_INIT_CLUSTER = "ha.allow_init_cluster";
    public static final String CONFIG_KEY_MAX_CONCURRENT_CHANNELS_PER_SLAVE = "ha.max_concurrent_channels_per_slave";
    public static final String CONFIG_KEY_BRANCHED_DATA_POLICY = "ha.branched_data_policy";
    public static final String CONFIG_KEY_READ_TIMEOUT = "ha.read_timeout";
    public static final String CONFIG_KEY_SLAVE_COORDINATOR_UPDATE_MODE = "ha.slave_coordinator_update_mode";
    public static final String CONFIG_KEY_LOCK_READ_TIMEOUT = "ha.lock_read_timeout";
    public static final String CONFIG_KEY_COORDINATOR_FETCH_INFO_TIMEOUT = "ha.coordinator_fetch_info_timeout";
    public static final String CONFIG_KEY_MAX_CONCURRENT_TRANSACTIONS_ON_MASTER = "ha.max_concurrent_transactions_on_master";
    public static final String CONFIG_KEY_ZK_SESSION_TIMEOUT = "ha.zk_session_timeout";

    public static final String CONFIG_DEFAULT_HA_CLUSTER_NAME = "neo4j.ha";
    public static final int CONFIG_DEFAULT_PORT = 6361;
    public static final long CONFIG_DEFAULT_PULL_INTERVAL = -1;
    public static final int CONFIG_DEFAULT_COORDINATOR_FETCH_INFO_TIMEOUT = 500;
    public static final long CONFIG_DEFAULT_ZK_SESSION_TIMEOUT = 5000;

    /**
     * The amount of memory to use for the node cache (when using the 'gcr'
     * cache).
     */
    @Documented
    public static final String NODE_CACHE_SIZE = "node_cache_size";

    /**
     * The amount of memory to use for the relationship cache (when using the
     * 'gcr' cache).
     */
    @Documented
    public static final String RELATIONSHIP_CACHE_SIZE = "relationship_cache_size";

    /**
     * The fraction of the heap (1%-10%) to use for the base array in the node
     * cache (when using the 'gcr' cache).
     */
    @Documented
    public static final String NODE_CACHE_ARRAY_FRACTION = "node_cache_array_fraction";

    /**
     * The fraction of the heap (1%-10%) to use for the base array in the
     * relationship cache (when using the 'gcr' cache).
     */
    @Documented
    public static final String RELATIONSHIP_CACHE_ARRAY_FRACTION = "relationship_cache_array_fraction";

    /**
     * The minimal time that must pass in between logging statistics from the
     * cache (when using the 'gcr' cache).
     * Default unit is seconds, suffix with 's', 'm', or 'ms' to have the unit
     * be seconds, minutes or milliseconds respectively.
     */
    @Documented
    public static final String GCR_CACHE_MIN_LOG_INTERVAL = "gcr_cache_min_log_interval";

    public static String getConfigValue( Map<String, String> config, String... oneKeyOutOf/*prioritized in descending order*/ )
    {
        String firstFound = null;
        int foundIndex = -1;
        for ( int i = 0; i < oneKeyOutOf.length; i++ )
        {
            String toTry = oneKeyOutOf[i];
            String value = config.get( toTry );
            if ( value != null )
            {
                if ( firstFound != null ) throw new RuntimeException( "Multiple configuration values set for the same logical key: " + asList( oneKeyOutOf ) );
                firstFound = value;
                foundIndex = i;
            }
        }
        if ( firstFound == null ) throw new RuntimeException( "No configuration set for any of: " + asList( oneKeyOutOf ) );
        if ( foundIndex > 0 ) System.err.println( "Deprecated configuration key '" + oneKeyOutOf[foundIndex] +
                "' used instead of the preferred '" + oneKeyOutOf[0] + "'" );
        return firstFound;
    }

    public static HAGraphDb.BranchedDataPolicy getBranchedDataPolicyFromConfig( Map<String, String> config )
    {
        return config.containsKey( HaConfig.CONFIG_KEY_BRANCHED_DATA_POLICY ) ?
                HAGraphDb.BranchedDataPolicy.valueOf( config.get( HaConfig.CONFIG_KEY_BRANCHED_DATA_POLICY ) ) :
                HAGraphDb.BranchedDataPolicy.keep_all;
    }

    public static SlaveUpdateMode getSlaveUpdateModeFromConfig( Map<String, String> config )
    {
        return config.containsKey( HaConfig.CONFIG_KEY_SLAVE_COORDINATOR_UPDATE_MODE ) ?
                SlaveUpdateMode.valueOf( config.get( HaConfig.CONFIG_KEY_SLAVE_COORDINATOR_UPDATE_MODE ) ) :
                SlaveUpdateMode.async;
    }

    public static int getClientReadTimeoutFromConfig( Map<String, String> config )
    {
        String value = config.get( HaConfig.CONFIG_KEY_READ_TIMEOUT );
        return value != null ? Integer.parseInt( value ) : Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS;
    }

    public static int getClientLockReadTimeoutFromConfig( Map<String, String> config )
    {
        String value = config.get( HaConfig.CONFIG_KEY_LOCK_READ_TIMEOUT );
        return value != null ? Integer.parseInt( value ) : getClientReadTimeoutFromConfig( config );
    }

    public static int getMaxConcurrentChannelsPerSlaveFromConfig( Map<String, String> config )
    {
        String value = config.get( HaConfig.CONFIG_KEY_MAX_CONCURRENT_CHANNELS_PER_SLAVE );
        return value != null ? Integer.parseInt( value ) : Client.DEFAULT_MAX_NUMBER_OF_CONCURRENT_CHANNELS_PER_CLIENT;
    }

    public static int getMaxConcurrentTransactionsOnMasterFromConfig( Map<String, String> config )
    {
        String value = config.get( HaConfig.CONFIG_KEY_MAX_CONCURRENT_TRANSACTIONS_ON_MASTER );
        return value != null ? Integer.parseInt( value ) : Server.DEFAULT_MAX_NUMBER_OF_CONCURRENT_TRANSACTIONS;
    }

    public static String getClusterNameFromConfig( Map<String, String> config )
    {
        String clusterName = config.get( HaConfig.CONFIG_KEY_CLUSTER_NAME );
        return clusterName != null ? clusterName : HaConfig.CONFIG_DEFAULT_HA_CLUSTER_NAME;
    }

    public static long getPullIntervalFromConfig( Map<String, String> config )
    {
        String value = config.get( HaConfig.CONFIG_KEY_PULL_INTERVAL );
        return value != null ? TimeUtil.parseTimeMillis( value ) : HaConfig.CONFIG_DEFAULT_PULL_INTERVAL;
    }

    public static long getZKSessionTimeoutFromConfig( Map<String, String> config )
    {
        String value = config.get( HaConfig.CONFIG_KEY_ZK_SESSION_TIMEOUT );
        return value != null ? TimeUtil.parseTimeMillis( value )
                : HaConfig.CONFIG_DEFAULT_ZK_SESSION_TIMEOUT;
    }

    public static String getHaServerFromConfig( Map<String, String> config )
    {
        String haServer = config.get( HaConfig.CONFIG_KEY_SERVER );
        if ( haServer == null )
        {
            InetAddress host = null;
            try
            {
                host = InetAddress.getLocalHost();
            }
            catch ( UnknownHostException hostBecomesNull )
            {
                // handled by null check
            }
            if ( host == null )
            {
                throw new IllegalStateException(
                        "Could not auto configure host name, please supply " + HaConfig.CONFIG_KEY_SERVER );
            }
            haServer = host.getHostAddress() + ":" + CONFIG_DEFAULT_PORT;
        }
        return haServer;
    }

    public static boolean getAllowInitFromConfig( Map<?, ?> config )
    {
        String allowInit = (String) config.get( HaConfig.CONFIG_KEY_ALLOW_INIT_CLUSTER );
        if ( allowInit == null ) return true;
        return Boolean.parseBoolean( allowInit );
    }

    public static String getCoordinatorsFromConfig( Map<String, String> config )
    {
        return getConfigValue( config, HaConfig.CONFIG_KEY_COORDINATORS, HaConfig.CONFIG_KEY_OLD_COORDINATORS );
    }

    public static int getMachineIdFromConfig( Map<String, String> config )
    {
        // Fail fast if null
        return Integer.parseInt( getConfigValue( config, HaConfig.CONFIG_KEY_SERVER_ID, HaConfig.CONFIG_KEY_OLD_SERVER_ID ) );
    }

    /**
     * @return the port for the backup server if that is enabled, or 0 if disabled.
     */
    public static int getBackupPortFromConfig( Map<?, ?> config )
    {
        String backupConfig = (String) config.get( ENABLE_ONLINE_BACKUP );
        Integer port = parsePort( backupConfig );
        return port != null ? port : 0;
    }

    public static int getFetchInfoTimeoutFromConfig( Map<String, String> config )
    {
        String value = config.get( CONFIG_KEY_COORDINATOR_FETCH_INFO_TIMEOUT );
        return value != null ? Integer.parseInt( value ) : CONFIG_DEFAULT_COORDINATOR_FETCH_INFO_TIMEOUT;
    }

    public static Map<String,String> loadConfigurations( String file )
    {
        return EmbeddedGraphDatabase.loadConfigurations( file );
    }

    public static enum SlaveUpdateMode
    {
        sync( true )
        {
            @Override
            public LastCommittedTxIdSetter createUpdater( Broker broker )
            {
                return new ZooKeeperLastCommittedTxIdSetter( broker );
            }
        },
        async( true )
        {
            @Override
            public LastCommittedTxIdSetter createUpdater( Broker broker )
            {
                return new AsyncZooKeeperLastCommittedTxIdSetter( broker );
            }
        },
        none( false )
        {
            @Override
            public LastCommittedTxIdSetter createUpdater( Broker broker )
            {
                return CommonFactories.defaultLastCommittedTxIdSetter();
            }
        };

        public final boolean syncWithZooKeeper;

        SlaveUpdateMode( boolean syncWithZooKeeper )
        {
            this.syncWithZooKeeper = syncWithZooKeeper;
        }

        public abstract LastCommittedTxIdSetter createUpdater( Broker broker );
    }
}
