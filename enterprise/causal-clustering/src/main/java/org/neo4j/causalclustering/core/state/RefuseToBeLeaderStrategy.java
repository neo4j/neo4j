/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;

import static org.neo4j.causalclustering.core.CausalClusteringSettings.multi_dc_license;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.refuse_to_be_leader;

/**
 * Simple utility class to abstract the interplay between multi dc feature licensing and
 * the refuse_to_be_leader config. refuse_to_be_leader should only be taken into consideration (i.e.
 * its value respected) when the multi dc license has been set to true. Otherwise, it will
 * always return false.
 */
public class RefuseToBeLeaderStrategy
{
    public static boolean shouldRefuseToBeLeader( Config config )
    {
        boolean multiDcLicensed = config.get( CausalClusteringSettings.multi_dc_license );
        boolean refuseToBeLeader = config.get( refuse_to_be_leader );

        return refuseToBeLeader && multiDcLicensed;
    }

    public static boolean shouldRefuseToBeLeader( Config config, Log log )
    {
        boolean multiDcLicensed = config.get( CausalClusteringSettings.multi_dc_license );
        boolean refuseToBeLeader = config.get( refuse_to_be_leader );

        if ( refuseToBeLeader && !multiDcLicensed )
        {
            /*
             * This means the user wants this instance to refuse to be leader but has not enabled multi_dc_license.
             * Issue a warning so they can see and fix that.
             */
            log.warn( String.format( "%s setting cannot be set to true unless %s is also set to true. Please refer to" +
                    " the Neo4j documentation for more information.", refuse_to_be_leader.name(), multi_dc_license
                    .name() ) );
        }

        return shouldRefuseToBeLeader( config );
    }
}
