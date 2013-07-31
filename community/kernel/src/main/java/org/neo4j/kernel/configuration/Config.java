/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Functions;
import org.neo4j.helpers.TimeUtil;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.info.DiagnosticsProvider;
import org.neo4j.kernel.logging.BufferingLogger;

import static java.lang.Character.isDigit;

/**
 * This class holds the overall configuration of a Neo4j database instance. Use the accessors
 * to convert the internal key-value settings to other types.
 * <p/>
 * Users can assume that old settings have been migrated to their new counterparts, and that defaults
 * have been applied.
 * <p/>
 * UI's can change configuration by calling applyChanges. Any listener, such as services that use
 * this configuration, can be notified of changes by implementing the {@link ConfigurationChangeListener} interface.
 */
public class Config implements DiagnosticsProvider
{
    private final List<ConfigurationChangeListener> listeners = new CopyOnWriteArrayList<ConfigurationChangeListener>();
    private final Map<String, String> params = new ConcurrentHashMap<String, String>(  );
    private final ConfigurationMigrator migrator;

    // Messages to this log get replayed into a real logger once logging has been
    // instantiated.
    private StringLogger log = new BufferingLogger();
    private final ConfigurationValidator validator;
    private final Function<String, String> settingsFunction;
    private final Iterable<Class<?>> settingsClasses;

    public Config()
    {
        this( new HashMap<String, String>(), Collections.<Class<?>>emptyList() );
    }

    public Config( Map<String, String> inputParams )
    {
        this( inputParams, Collections.<Class<?>>emptyList() );
    }

    public Config( Map<String, String> inputParams, Class<?>... settingsClasses )
    {
        this( inputParams, Arrays.asList( settingsClasses ) );
    }

    public Config( Map<String, String> inputParams, Iterable<Class<?>> settingsClasses )
    {
        this.settingsClasses = settingsClasses;
        settingsFunction = Functions.map( params );

        this.migrator = new AnnotationBasedConfigurationMigrator( settingsClasses );
        this.validator = new ConfigurationValidator( settingsClasses );
        this.applyChanges( inputParams );
    }

    // TODO: Get rid of this, to allow us to have something more
    // elaborate as internal storage (eg. something that can keep meta data with
    // properties).
    public Map<String, String> getParams()
    {
        return new HashMap<String, String>( this.params );
    }

    /**
     * Retrieve a configuration property.
     */
    public <T> T get( Setting<T> setting )
    {
        return setting.apply( settingsFunction );
    }

    /**
     * Replace the current set of configuration parameters with another one.
     */
    public synchronized void applyChanges( Map<String, String> newConfiguration )
    {
        newConfiguration = migrator.apply( newConfiguration, log );

        // Make sure all changes are valid
        validator.validate( newConfiguration );

        // Figure out what changed
        if ( listeners.isEmpty() )
        {
            // Make the change
            params.clear();
            params.putAll( newConfiguration );
        }
        else
        {
            List<ConfigurationChange> configurationChanges = new ArrayList<ConfigurationChange>();
            for ( Map.Entry<String, String> stringStringEntry : newConfiguration.entrySet() )
            {
                String oldValue = params.get( stringStringEntry.getKey() );
                String newValue = stringStringEntry.getValue();
                if ( !(oldValue == null && newValue == null) &&
                        (oldValue == null || newValue == null || !oldValue.equals( newValue )) )
                {
                    configurationChanges.add( new ConfigurationChange( stringStringEntry.getKey(), oldValue,
                            newValue ) );
                }
            }

            // Make the change
            params.clear();
            params.putAll( newConfiguration );

            // Notify listeners
            for ( ConfigurationChangeListener listener : listeners )
            {
                listener.notifyConfigurationChanges( configurationChanges );
            }
        }
    }

    public Iterable<Class<?>> getSettingsClasses()
    {
        return settingsClasses;
    }

    public void setLogger( StringLogger log )
    {
        if ( this.log instanceof BufferingLogger )
        {
            ((BufferingLogger) this.log).replayInto( log );
        }
        this.log = log;
    }

    public void addConfigurationChangeListener( ConfigurationChangeListener listener )
    {
        listeners.add( listener );
    }

    public void removeConfigurationChangeListener( ConfigurationChangeListener listener )
    {
        listeners.remove( listener );
    }

    @Override
    public String getDiagnosticsIdentifier()
    {
        return getClass().getName();
    }

    @Override
    public void acceptDiagnosticsVisitor( Object visitor )
    {
        // nothing visits configuration
    }

    @Override
    public void dump( DiagnosticsPhase phase, StringLogger log )
    {
        if ( phase.isInitialization() || phase.isExplicitlyRequested() )
        {
            log.logLongMessage( "Neo4j Kernel properties:", Iterables.map( new Function<Map.Entry<String, String>,
                    String>()
            {
                @Override
                public String apply( Map.Entry<String, String> stringStringEntry )
                {
                    return stringStringEntry.getKey() + "=" + stringStringEntry.getValue();
                }
            }, params.entrySet() ) );
        }
    }

    @Override
    public String toString()
    {
        List<String> keys = new ArrayList<String>( params.keySet() );
        Collections.sort( keys );
        LinkedHashMap<String, String> output = new LinkedHashMap<String, String>();
        for ( String key : keys )
        {
            output.put( key, params.get( key ) );
        }

        return output.toString();
    }

    //
    // To be removed in 1.10
    //

    @Deprecated
    static final String NIO_NEO_DB_CLASS = "org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource";
    @Deprecated
    public static final String DEFAULT_DATA_SOURCE_NAME = "nioneodb";

    @Deprecated
    static final String LUCENE_DS_CLASS = "org.neo4j.index.lucene.LuceneDataSource";
    @Deprecated
    static final String LUCENE_FULLTEXT_DS_CLASS = "org.neo4j.index.lucene.LuceneFulltextDataSource";

    /**
     * Tell Neo4j to use memory mapped buffers for accessing the native storage
     * layer
     */
    @Documented
    @Deprecated
    public static final String USE_MEMORY_MAPPED_BUFFERS = "use_memory_mapped_buffers";

    /**
     * Print out the effective Neo4j configuration after startup
     */
    @Documented
    @Deprecated
    public static final String DUMP_CONFIGURATION = "dump_configuration";

    /**
     * Make Neo4j keep the logical transaction logs for being able to backup the
     * database. Provides control over how much disk space logical logs are allowed
     * to take per data source.
     */
    @Documented
    @Deprecated
    public static final String KEEP_LOGICAL_LOGS = "keep_logical_logs";
    /**
     * Enable a remote shell server which shell clients can log in to
     */
    @Documented
    @Deprecated
    public static final String ENABLE_REMOTE_SHELL = "enable_remote_shell";

    /**
     * Enable a support for running online backups
     */
    @Documented
    @Deprecated
    public static final String ENABLE_ONLINE_BACKUP = "enable_online_backup";

    /**
     * Mark this database as a backup slave.
     */
    @Documented
    @Deprecated
    public static final String BACKUP_SLAVE = "backup_slave";

    /**
     * Only allow read operations from this Neo4j instance.
     */
    @Documented
    @Deprecated
    public static final String READ_ONLY = "read_only";
    /**
     * Relative path for where the Neo4j storage directory is located
     */
    @Documented
    @Deprecated
    public static final String STORAGE_DIRECTORY = "store_dir";
    /**
     * Use a quick approach for rebuilding the ID generators. This give quicker
     * recovery time, but will limit the ability to reuse the space of deleted
     * entities.
     */
    @Documented
    @Deprecated
    public static final String REBUILD_IDGENERATORS_FAST = "rebuild_idgenerators_fast";
    /**
     * The size to allocate for memory mapping the node store
     */
    @Documented
    @Deprecated
    public static final String NODE_STORE_MMAP_SIZE = "neostore.nodestore.db.mapped_memory";
    /**
     * The size to allocate for memory mapping the array property store
     */
    @Documented
    @Deprecated
    public static final String ARRAY_PROPERTY_STORE_MMAP_SIZE = "neostore.propertystore.db.arrays.mapped_memory";
    /**
     * The size to allocate for memory mapping the store for property key
     * strings
     */
    @Documented
    @Deprecated
    public static final String PROPERTY_INDEX_KEY_STORE_MMAP_SIZE = "neostore.propertystore.db.index.keys" +
            ".mapped_memory";
    /**
     * The size to allocate for memory mapping the store for property key
     * indexes
     */
    @Documented
    @Deprecated
    public static final String PROPERTY_INDEX_STORE_MMAP_SIZE = "neostore.propertystore.db.index.mapped_memory";
    /**
     * The size to allocate for memory mapping the property value store
     */
    @Documented
    @Deprecated
    public static final String PROPERTY_STORE_MMAP_SIZE = "neostore.propertystore.db.mapped_memory";
    /**
     * The size to allocate for memory mapping the string property store
     */
    @Documented
    @Deprecated
    public static final String STRING_PROPERTY_STORE_MMAP_SIZE = "neostore.propertystore.db.strings.mapped_memory";
    /**
     * The size to allocate for memory mapping the relationship store
     */
    @Documented
    @Deprecated
    public static final String RELATIONSHIP_STORE_MMAP_SIZE = "neostore.relationshipstore.db.mapped_memory";
    /**
     * Relative path for where the Neo4j logical log is located
     */
    @Documented
    @Deprecated
    public static final String LOGICAL_LOG = "logical_log";
    /**
     * Relative path for where the Neo4j storage information file is located
     */
    @Documented
    @Deprecated
    public static final String NEO_STORE = "neo_store";

    /**
     * The type of cache to use for nodes and relationships, one of [weak, soft,
     * none, array]
     */
    @Documented
    @Deprecated
    public static final String CACHE_TYPE = "cache_type";

    /**
     * The amount of memory to use for the node cache (when using the 'gcr' cache).
     */
    @Documented
    @Deprecated
    public static final String NODE_CACHE_SIZE = "node_cache_size";

    /**
     * The amount of memory to use for the relationship cache (when using the 'gcr' cache).
     */
    @Documented
    @Deprecated
    public static final String RELATIONSHIP_CACHE_SIZE = "relationship_cache_size";

    /**
     * The fraction of the heap (1%-10%) to use for the base array in the node cache (when using the 'gcr' cache).
     */
    @Documented
    @Deprecated
    public static final String NODE_CACHE_ARRAY_FRACTION = "node_cache_array_fraction";

    /**
     * The fraction of the heap (1%-10%) to use for the base array in the relationship cache (when using the 'gcr'
     * cache).
     */
    @Documented
    @Deprecated
    public static final String RELATIONSHIP_CACHE_ARRAY_FRACTION = "relationship_cache_array_fraction";

    /**
     * The minimal time that must pass in between logging statistics from the cache (when using the 'gcr' cache).
     * Default unit is seconds, suffix with 's', 'm', or 'ms' to have the unit be seconds,
     * minutes or milliseconds respectively.
     */
    @Documented
    @Deprecated
    public static final String GCR_CACHE_MIN_LOG_INTERVAL = "gcr_cache_min_log_interval";

    /**
     * The name of the Transaction Manager service to use as defined in the TM
     * service provider constructor, defaults to native.
     */
    @Documented
    @Deprecated
    public static final String TXMANAGER_IMPLEMENTATION = "tx_manager_impl";

    /**
     * Determines whether any TransactionInterceptors loaded will intercept
     * prepared transactions before they reach the logical log. Defaults to
     * false.
     */
    @Documented
    @Deprecated
    public static final String INTERCEPT_COMMITTING_TRANSACTIONS = "intercept_committing_transactions";

    /**
     * Determines whether any TransactionInterceptors loaded will intercept
     * externally received transactions (e.g. in HA) before they reach the
     * logical log and are applied to the store. Defaults to false.
     */
    @Documented
    @Deprecated
    public static final String INTERCEPT_DESERIALIZED_TRANSACTIONS = "intercept_deserialized_transactions";

    /**
     * Boolean (one of true,false) defining whether to allow a store upgrade
     * in case the current version of the database starts against an older store
     * version. Setting this to true does not guarantee successful upgrade, just
     * allows an attempt at it.
     */
    @Documented
    @Deprecated
    public static final String ALLOW_STORE_UPGRADE = "allow_store_upgrade";
    @Deprecated
    public static final String STRING_BLOCK_SIZE = "string_block_size";
    @Deprecated
    public static final String ARRAY_BLOCK_SIZE = "array_block_size";

    /**
     * A list of property names (comma separated) that will be indexed by
     * default.
     * This applies to Nodes only.
     */
    @Documented
    @Deprecated
    public static final String NODE_KEYS_INDEXABLE = "node_keys_indexable";
    /**
     * A list of property names (comma separated) that will be indexed by
     * default.
     * This applies to Relationships only.
     */
    @Documented
    @Deprecated
    public static final String RELATIONSHIP_KEYS_INDEXABLE = "relationship_keys_indexable";

    /**
     * Boolean value (one of true, false) that controls the auto indexing
     * feature for nodes. Setting to false shuts it down unconditionally,
     * while true enables it for every property, subject to restrictions
     * in the configuration.
     * The default is false.
     */
    @Documented
    @Deprecated
    public static final String NODE_AUTO_INDEXING = "node_auto_indexing";

    /**
     * Boolean value (one of true, false) that controls the auto indexing
     * feature for relationships. Setting to false shuts it down
     * unconditionally, while true enables it for every property, subject
     * to restrictions in the configuration.
     * The default is false.
     */
    @Documented
    @Deprecated
    public static final String RELATIONSHIP_AUTO_INDEXING = "relationship_auto_indexing";

    /**
     * Integer value that sets the maximum number of open lucene index searchers.
     * The default is Integer.MAX_VALUE
     */
    @Documented
    @Deprecated
    public static final String LUCENE_SEARCHER_CACHE_SIZE = "lucene_searcher_cache_size";

    /**
     * Integer value that sets the maximum number of open lucene index writers.
     * The default is Integer.MAX_VALUE
     */
    @Documented
    @Deprecated
    public static final String LUCENE_WRITER_CACHE_SIZE = "lucene_writer_cache_size";

    /**
     * Amount of time in ms the GC monitor thread will wait before taking another measurement.
     * Default is 100 ms.
     */
    @Documented
    @Deprecated
    public static final String GC_MONITOR_WAIT_TIME = "gc_monitor_wait_time";

    /**
     * The amount of time in ms the monitor thread has to be blocked before logging a message it was blocked.
     * Default is 200ms
     */
    @Documented
    @Deprecated
    public static final String GC_MONITOR_THRESHOLD = "gc_monitor_threshold";

    @Deprecated
    static final String LOAD_EXTENSIONS = "load_kernel_extensions";

    @Deprecated
    public boolean getBoolean( GraphDatabaseSetting.BooleanSetting setting )
    {
        return get( setting );
    }

    @Deprecated
    public int getInteger( GraphDatabaseSetting.IntegerSetting setting )
    {
        return get( setting );
    }

    @Deprecated
    public long getLong( GraphDatabaseSetting.LongSetting setting )
    {
        return get( setting );
    }

    @Deprecated
    public double getDouble( GraphDatabaseSetting.DoubleSetting setting )
    {
        return get( setting );
    }

    @Deprecated
    public float getFloat( GraphDatabaseSetting.FloatSetting setting )
    {
        return get( setting );
    }

    @Deprecated
    public long getSize( GraphDatabaseSetting.StringSetting setting )
    {
        return parseLongWithUnit( get( setting ) );
    }

    public static long parseLongWithUnit( String numberWithPotentialUnit )
    {
        int firstNonDigitIndex = findFirstNonDigit( numberWithPotentialUnit );
        String number = numberWithPotentialUnit.substring( 0, firstNonDigitIndex );
        
        long multiplier = 1;
        if ( firstNonDigitIndex < numberWithPotentialUnit.length() )
        {
            String unit = numberWithPotentialUnit.substring( firstNonDigitIndex );
            if ( unit.equalsIgnoreCase( "k" ) )
            {
                multiplier = 1024;
            }
            else if ( unit.equalsIgnoreCase( "m" ) )
            {
                multiplier = 1024 * 1024;
            }
            else if ( unit.equalsIgnoreCase( "g" ) )
            {
                multiplier = 1024 * 1024 * 1024;
            }
            else
            {
                throw new IllegalArgumentException(
                        "Illegal unit '" + unit + "' for number '" + numberWithPotentialUnit + "'" );
            }
        }
        
        return Long.parseLong( number ) * multiplier;
    }

    /**
     * @return index of first non-digit character in {@code numberWithPotentialUnit}. If all digits then
     * {@code numberWithPotentialUnit.length()} is returned.
     */
    private static int findFirstNonDigit( String numberWithPotentialUnit )
    {
        int firstNonDigitIndex = numberWithPotentialUnit.length();
        for ( int i = 0; i < numberWithPotentialUnit.length(); i++ )
        {
            if ( !isDigit( numberWithPotentialUnit.charAt( i ) ) )
            {
                firstNonDigitIndex = i;
                break;
            }
        }
        return firstNonDigitIndex;
    }

    @Deprecated
    public long getDuration( GraphDatabaseSetting.StringSetting setting )
    {
        return TimeUtil.parseTimeMillis.apply( get( setting ) );
    }

    @Deprecated
    public <T extends Enum<T>> T getEnum( Class<T> enumType,
                                          GraphDatabaseSetting.OptionsSetting graphDatabaseSetting )
    {
        return Enum.valueOf( enumType, get( graphDatabaseSetting ) );
    }
}
