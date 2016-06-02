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
package org.neo4j.kernel.impl.transaction.log.pruning;

import static org.neo4j.kernel.configuration.Settings.parseLongWithUnit;

public class ThresholdConfigParser
{
    public static final class ThresholdConfigValue
    {
        public static final ThresholdConfigValue NO_PRUNING = new ThresholdConfigValue( "false", -1 );
        public final String type;
        public final long value;

        public ThresholdConfigValue( String type, long value )
        {
            this.type = type;
            this.value = value;
        }
    }

    public static ThresholdConfigValue parse( String configValue )
    {
        String[] tokens = configValue.split( " " );
        if ( tokens.length == 0 )
        {
            throw new IllegalArgumentException( "Invalid log pruning configuration value '" + configValue + "'" );
        }

        final String boolOrNumber = tokens[0];

        if ( tokens.length == 1 )
        {
            switch ( boolOrNumber )
            {
            case "keep_all":
            case "true":
                return ThresholdConfigValue.NO_PRUNING;
            case "keep_none":
            case "false":
                return new ThresholdConfigValue( "entries", 1 );
            default:
                throw new IllegalArgumentException( "Invalid log pruning configuration value '" + configValue +
                        "'. The form is 'all' or '<number><unit> <type>' for example '100k txs' " +
                        "for the latest 100 000 transactions" );
            }
        }

        long thresholdValue = parseLongWithUnit( tokens[0] );
        String thresholdType = tokens[1];

        return new ThresholdConfigValue( thresholdType, thresholdValue );
    }
}
