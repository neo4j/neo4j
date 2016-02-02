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
import org.neo4j.kernel.impl.store.format.lowlimit.LowLimitV1_9;
import org.neo4j.kernel.impl.store.format.lowlimit.LowLimitV2_0;
import org.neo4j.kernel.impl.store.format.lowlimit.LowLimitV2_1;
import org.neo4j.kernel.impl.store.format.lowlimit.LowLimitV2_2;
import org.neo4j.kernel.impl.store.format.lowlimit.LowLimitV2_3;
import org.neo4j.kernel.impl.store.format.lowlimit.LowLimitV3_0;

/**
 * Selects format to use for databases in this JVM, using a system property. By default uses the safest
 * and established format. During development this may be switched in builds to experimental formats
 * to gain more testing there.
 */
public class InternalRecordFormatSelector
{
    private static final RecordFormats FALLBACK = LowLimitV3_0.RECORD_FORMATS;

    private static final RecordFormats[] KNOWN_FORMATS = new RecordFormats[] {
            LowLimitV1_9.RECORD_FORMATS,
            LowLimitV2_0.RECORD_FORMATS,
            LowLimitV2_1.RECORD_FORMATS,
            LowLimitV2_2.RECORD_FORMATS,
            LowLimitV2_3.RECORD_FORMATS,
            LowLimitV3_0.RECORD_FORMATS
    };

    public static RecordFormats select()
    {
        return select( Config.empty(), NullLogService.getInstance() );
    }

    public static RecordFormats select( Config config, LogService logging )
    {
        String key = config.get( GraphDatabaseFacadeFactory.Configuration.record_format );
        for ( RecordFormats.Factory candidate : Service.load( RecordFormats.Factory.class ) )
        {
            String candidateId = candidate.getKeys().iterator().next();
            if ( candidateId.equals( key ) )
            {
                return FALLBACK;
                //todo: uncomment and return correct format after migration PR is merged.
                //return candidate.newInstance();
            }
            else if ( key.equals( "" ) )
            {
                logging.getInternalLog( CommunityFacadeFactory.class )
                        .info( "No record format specified, defaulting to '" + candidateId + "'" );
                return FALLBACK;
                //todo: uncomment and return correct format after migration PR is merged.
                //return candidate.newInstance();
            }
        }

        if ( key.equals( "" ) )
        {
            logging.getInternalLog( CommunityFacadeFactory.class )
                    .info( "No record format specified, defaulting to 'community'" );
            return FALLBACK;
        }
        else if ( key.equals( "community" ) )
        {
            return FALLBACK;
        }

        throw new IllegalArgumentException( "No record format found with the name '" + key + "'." );
    }

    public static RecordFormats fromVersion( String storeVersion )
    {
        for ( RecordFormats format : KNOWN_FORMATS )
        {
            if ( format.storeVersion().equals( storeVersion ) )
            {
                return format;
            }
        }
        throw new IllegalArgumentException( "Unknown store version '" + storeVersion + "'" );
    }
}
