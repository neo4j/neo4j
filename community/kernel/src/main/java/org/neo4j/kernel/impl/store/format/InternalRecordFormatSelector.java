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

import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.CommunityFacadeFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.format.RecordFormats.Factory;
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
public class InternalRecordFormatSelector
{
    private static final String COMMUNITY_KEY = LowLimitV3_0.NAME;
    private static final RecordFormats FALLBACK = LowLimitV3_0.RECORD_FORMATS;
    private static final Iterable<RecordFormats> KNOWN_FORMATS = asList(
            LowLimitV2_0.RECORD_FORMATS,
            LowLimitV2_1.RECORD_FORMATS,
            LowLimitV2_2.RECORD_FORMATS,
            LowLimitV2_3.RECORD_FORMATS,
            LowLimitV3_0.RECORD_FORMATS
    );

    private static Iterable<Factory> loadAdditionalFormats()
    {
        return Service.load( RecordFormats.Factory.class );
    }

    public static RecordFormats select()
    {
        return select( Config.empty(), NullLogService.getInstance() );
    }

    public static RecordFormats select( Config config, LogService logging )
    {
        String key = config.get( GraphDatabaseFacadeFactory.Configuration.record_format );
        for ( RecordFormats.Factory candidate : loadAdditionalFormats() )
        {
            String candidateId = candidate.getKeys().iterator().next();
            if ( candidateId.equals( key ) )
            {
                // We specified config to select this format specifically and we found it
                return candidate.newInstance();
            }
            else if ( "".equals( key ) )
            {
                // No specific config, just return the first format we found from service loading
                return loggedSelection( candidate.newInstance(), candidateId, logging );
            }
        }

        if ( COMMUNITY_KEY.equals( key ) )
        {
            // We specified config to select the community format
            return FALLBACK;
        }
        if ( "".equals( key ) )
        {
            // No specific config, just return the community fallback
            return loggedSelection( FALLBACK, COMMUNITY_KEY, logging );
        }

        throw new IllegalArgumentException( "No record format found with the name '" + key + "'." );
    }

    private static RecordFormats loggedSelection( RecordFormats format, String candidateId, LogService logging )
    {
        logging.getInternalLog( CommunityFacadeFactory.class )
                .info( "No record format specified, defaulting to '" + candidateId + "'" );
        return format;
    }

    public static RecordFormats fromVersion( String storeVersion )
    {
        Iterable<RecordFormats> additionalFormats = map( Factory::newInstance,
                loadAdditionalFormats() );
        for ( RecordFormats format : concat( KNOWN_FORMATS, additionalFormats ) )
        {
            if ( format.storeVersion().equals( storeVersion ) )
            {
                return format;
            }
        }
        throw new IllegalArgumentException( "Unknown store version '" + storeVersion + "'" );
    }
}
