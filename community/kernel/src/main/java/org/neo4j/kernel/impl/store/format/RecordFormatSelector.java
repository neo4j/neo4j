/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format;

import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.format.lowlimit.LowLimitV2_0;
import org.neo4j.kernel.impl.store.format.lowlimit.LowLimitV2_1;
import org.neo4j.kernel.impl.store.format.lowlimit.LowLimitV2_2;
import org.neo4j.kernel.impl.store.format.lowlimit.LowLimitV2_3;
import org.neo4j.kernel.impl.store.format.lowlimit.LowLimitV3_0;
import org.neo4j.logging.Log;

import static java.util.Arrays.asList;
import static org.neo4j.helpers.collection.Iterables.concat;
import static org.neo4j.helpers.collection.Iterables.map;

/**
 * Selects record format that will be used in a database.
 * Support two types of selection : config based or automatic.
 * <p>
 * Automatic selection is used by various tools and tests that should work for format independent (for example backup)
 */
public class RecordFormatSelector
{

    private static final RecordFormats DEFAULT_AUTOSELECT_FORMAT = LowLimitV3_0.RECORD_FORMATS;

    private static final Iterable<RecordFormats> KNOWN_FORMATS = asList(
            LowLimitV2_0.RECORD_FORMATS,
            LowLimitV2_1.RECORD_FORMATS,
            LowLimitV2_2.RECORD_FORMATS,
            LowLimitV2_3.RECORD_FORMATS,
            LowLimitV3_0.RECORD_FORMATS
    );

    /**
     * Select record formats based on provided format name in
     * {@link GraphDatabaseFacadeFactory.Configuration#record_format} property
     *
     * @param config database configuration
     * @param logService logging service
     * @return configured record formats
     * @throws IllegalArgumentException if requested format not found
     */
    public static RecordFormats select( Config config, LogService logService )
    {
        String recordFormat = configuredRecordFormat( config );
        RecordFormats formats = loadRecordFormat( recordFormat );
        if ( formats == null )
        {
            handleMissingFormat( recordFormat, logService );
        }
        logSelectedFormat( logService, formats );
        return formats;
    }

    /**
     * Select record formats based on provided format name in
     * {@link GraphDatabaseFacadeFactory.Configuration#record_format} property
     * If property not specified provided defaultFormat will be return instead.
     *
     * @param config database configuration
     * @param defaultFormat default format
     * @param logService logging service
     * @return configured or default record format
     * @throws IllegalArgumentException if requested format not found
     */
    public static RecordFormats select( Config config, RecordFormats defaultFormat, LogService logService )
    {
        RecordFormats selectedFormat = defaultFormat;
        String recordFormat = configuredRecordFormat( config );
        if ( StringUtils.isNotEmpty( recordFormat ) )
        {
            selectedFormat = selectSpecificFormat( recordFormat, logService );
        }
        logSelectedFormat( logService, selectedFormat );
        return selectedFormat;
    }

    /**
     * Select record formats for provided store version.
     *
     * @param storeVersion store version to find format for
     * @return record formats
     * @throws IllegalArgumentException if format for specified store version not found
     */
    public static RecordFormats selectForVersion( String storeVersion )
    {
        Iterable<RecordFormats> currentFormats =
                map( RecordFormats.Factory::newInstance, Service.load( RecordFormats.Factory.class ) );
        for ( RecordFormats format : concat( KNOWN_FORMATS, currentFormats ) )
        {
            if ( format.storeVersion().equals( storeVersion ) )
            {
                return format;
            }
        }
        throw new IllegalArgumentException( "Unknown store version '" + storeVersion + "'" );
    }

    /**
     * Select record formats based on available services in class path.
     * In case if multiple services are available only 'first' will be considered.
     * In case if no record format providers are found - default record format ({@link #DEFAULT_AUTOSELECT_FORMAT}) will
     * be used.
     *
     * @return - selected record format.
     */
    public static RecordFormats autoSelectFormat()
    {
        return autoSelectFormat( Config.empty(), NullLogService.getInstance() );
    }

    /**
     * Select record formats based on available services in class path.
     * Specific format selection can be forced by specifying format name in
     * {@link GraphDatabaseFacadeFactory.Configuration#record_format} property.
     * In case if multiple services are available only 'first' will be considered.
     * In case if no record format providers are found - default record format ({@link #DEFAULT_AUTOSELECT_FORMAT}) will
     * be used.
     *
     * @param config - configuration parameters
     * @param logService - logging service
     * @return - selected record format.
     * @throws IllegalArgumentException if specific requested format not found
     */
    public static RecordFormats autoSelectFormat( Config config, LogService logService )
    {
        String recordFormat = configuredRecordFormat( config );
        RecordFormats recordFormats = StringUtils.isNotEmpty( recordFormat ) ?
                                      selectSpecificFormat( recordFormat, logService ) :
                                      getFirstAvailableOrDefault( DEFAULT_AUTOSELECT_FORMAT );
        logSelectedFormat( logService, recordFormats );
        return recordFormats;
    }

    private static RecordFormats selectSpecificFormat( String recordFormat, LogService logService )
    {
        RecordFormats formats = loadRecordFormat( recordFormat );
        if ( formats == null )
        {
            return handleMissingFormat( recordFormat, logService );
        }
        return formats;
    }

    private static RecordFormats getFirstAvailableOrDefault( RecordFormats defaultAutoSelectFormat )
    {
        Iterable<RecordFormats.Factory> formatFactories = Service.load( RecordFormats.Factory.class );
        Iterator<RecordFormats.Factory> factoryIterator = formatFactories.iterator();
        return factoryIterator.hasNext() ? factoryIterator.next().newInstance() : defaultAutoSelectFormat;
    }

    private static RecordFormats loadRecordFormat( String recordFormat )
    {
        if ( StringUtils.isNotEmpty( recordFormat ) )
        {
            if ( LowLimitV3_0.NAME.equals( recordFormat ) )
            {
                return LowLimitV3_0.RECORD_FORMATS;
            }
            RecordFormats.Factory formatFactory = Service.loadSilently( RecordFormats.Factory.class, recordFormat );
            if ( formatFactory != null )
            {
                return formatFactory.newInstance();
            }
        }
        return null;
    }

    private static void logSelectedFormat( LogService logService, RecordFormats formats )
    {
        String selectionMessage =
                String.format( "Select %s as record format implementation.", formats.getClass().getName() );
        getLog( logService ).warn( selectionMessage );
    }

    private static RecordFormats handleMissingFormat( String recordFormat, LogService logService )
    {
        getLog( logService ).warn( "Record format with key '" + recordFormat + "' not found." );
        throw new IllegalArgumentException( "No record format found with the name '" + recordFormat + "'." );
    }

    private static Log getLog( LogService logService )
    {
        return logService.getInternalLog( RecordFormatSelector.class );
    }

    private static String configuredRecordFormat( Config config )
    {
        return config.get( GraphDatabaseFacadeFactory.Configuration.record_format );
    }
}
