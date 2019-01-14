/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.log.pruning;

import java.util.Objects;

import static org.neo4j.kernel.configuration.Settings.parseLongWithUnit;

public class ThresholdConfigParser
{
    public static final class ThresholdConfigValue
    {
        static final ThresholdConfigValue NO_PRUNING = new ThresholdConfigValue( "false", -1 );
        static final ThresholdConfigValue KEEP_LAST_FILE = new ThresholdConfigValue( "entries", 1 );

        public final String type;
        public final long value;

        ThresholdConfigValue( String type, long value )
        {
            this.type = type;
            this.value = value;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            ThresholdConfigValue that = (ThresholdConfigValue) o;
            return value == that.value && Objects.equals( type, that.type );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( type, value );
        }
    }

    private ThresholdConfigParser()
    {
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
                return ThresholdConfigValue.KEEP_LAST_FILE;
            default:
                throw new IllegalArgumentException( "Invalid log pruning configuration value '" + configValue +
                        "'. The form is 'true', 'false' or '<number><unit> <type>'. For example, '100k txs' " +
                        "will keep the 100 000 latest transactions." );
            }
        }
        else
        {
            long thresholdValue = parseLongWithUnit( boolOrNumber );
            String thresholdType = tokens[1];
            return new ThresholdConfigValue( thresholdType, thresholdValue );
        }
    }
}
