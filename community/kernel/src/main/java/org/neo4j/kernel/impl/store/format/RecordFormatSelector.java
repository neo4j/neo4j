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

import static java.util.Arrays.asList;
import static org.neo4j.helpers.collection.Iterables.concat;
import static org.neo4j.helpers.collection.Iterables.map;

/**
 * Selects format to use for databases in this JVM, using a system property. By default uses the safest
 * and established format. During development this may be switched in builds to experimental formats
 * to gain more testing there.
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
     * @throws IllegalAccessException if requested format not found
     */
    public static RecordFormats select( Config config, LogService logService )
    {
        String recordFormat = configuredRecordFormat( config );
        RecordFormats formats = loadRecordFormat( recordFormat );
        if ( formats == null )
        {
            handleMissingFormat( recordFormat, logService );
        }
        return formats;
    }

    /**
     * Select record formats based on provided format name in
     * {@link GraphDatabaseFacadeFactory.Configuration#record_format} property
     * If property not specified provided defaultFormat will be return instead.
     *
     * @param config database configuration
     * @param defaultFormat default format
     * @param logService loggin service
     * @return configured or default record format
     * @throws IllegalAccessException if requested format not found
     */
    public static RecordFormats select( Config config, RecordFormats defaultFormat,
            LogService logService )
    {
        String recordFormat = configuredRecordFormat( config );
        if ( StringUtils.isNotEmpty( recordFormat ) )
        {
            RecordFormats formats = loadRecordFormat( recordFormat );
            if ( formats == null )
            {
                return handleMissingFormat( recordFormat, logService );
            }
            return formats;
        }
        return defaultFormat;
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

    private static RecordFormats loadRecordFormat( String recordFormat )
    {
        if ( StringUtils.isNotEmpty( recordFormat ) )
        {
            RecordFormats.Factory formatFactory = Service.loadSilently( RecordFormats.Factory.class, recordFormat );
            if ( formatFactory != null )
            {
                return formatFactory.newInstance();
            }
        }
        return null;
    }

    public static RecordFormats autoSelectFormat()
    {
        return autoSelectFormat( Config.empty(), NullLogService.getInstance() );
    }

    public static RecordFormats autoSelectFormat( Config config, LogService logService )
    {
        String recordFormat = configuredRecordFormat( config );
        RecordFormats formats = loadRecordFormat( recordFormat );
        if ( formats != null )
        {
            logService.getInternalLog( RecordFormatSelector.class ).warn( "Selected " +
                                                     recordFormat.getClass().getName() + " record format." );
            return formats;
        }
        Iterable<RecordFormats.Factory> formatFactories = Service.load( RecordFormats.Factory.class );
        Iterator<RecordFormats.Factory> factoryIterator = formatFactories.iterator();
        RecordFormats recordFormats = factoryIterator.hasNext() ? factoryIterator.next().newInstance() : DEFAULT_AUTOSELECT_FORMAT;
        logService.getInternalLog( RecordFormatSelector.class ).warn( "Selected " +
                                                                      recordFormats.getClass().getName() + " record " +
                                                                      "format." );
        return recordFormats;
    }

    private static RecordFormats handleMissingFormat( String recordFormat, LogService logService )
    {
        logService.getInternalLog( RecordFormatSelector.class )
                .warn( "Record format with key '" + recordFormat + "' not found." );
        throw new IllegalArgumentException( "No record format found with the name '" + recordFormat + "'." );
    }

    private static String configuredRecordFormat( Config config )
    {
        return config.get( GraphDatabaseFacadeFactory.Configuration.record_format );
    }
}
