/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.impl.query;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.neo4j.helpers.Strings;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.values.AnyValue;
import org.neo4j.values.utils.PrettyPrinter;
import org.neo4j.values.virtual.MapValue;

class QueryLogFormatter
{
    private QueryLogFormatter()
    {
    }

    static void formatPageDetails( StringBuilder result, QuerySnapshot query )
    {
        result.append( query.pageHits() ).append( " page hits, " );
        result.append( query.pageFaults() ).append( " page faults - " );
    }

    static void formatAllocatedBytes( StringBuilder result, QuerySnapshot query )
    {
        Long bytes = query.allocatedBytes();
        if ( bytes != null )
        {
            result.append( bytes ).append( " B - " );
        }
    }

    static void formatDetailedTime( StringBuilder result, QuerySnapshot query )
    {
        result.append( "(planning: " ).append( query.planningTimeMillis() );
        Long cpuTime = query.cpuTimeMillis();
        if ( cpuTime != null )
        {
            result.append( ", cpu: " ).append( cpuTime );
        }
        result.append( ", waiting: " ).append( query.waitTimeMillis() );
        result.append( ") - " );
    }

    static void formatMapValue( StringBuilder result, MapValue params )
    {
        formatMapValue( result, params, Collections.emptySet() );
    }

    static void formatMapValue( StringBuilder result, MapValue params, Collection<String> obfuscate )
    {
        result.append( '{' );
        if ( params != null )
        {
            String sep = "";
            for ( Map.Entry<String,AnyValue> entry : params.entrySet() )
            {
                result
                        .append( sep )
                        .append( entry.getKey() )
                        .append( ": " );

                if ( obfuscate.contains( entry.getKey() ) )
                {
                    result.append( "******" );
                }
                else
                {
                    result.append( formatAnyValue( entry.getValue() ));
                }
                sep = ", ";
            }
        }
        result.append( "}" );
    }

    static String formatAnyValue( AnyValue value )
    {
        PrettyPrinter printer = new PrettyPrinter( "'" );
        value.writeTo( printer );
        return printer.value();
    }

    static void formatMap( StringBuilder result, Map<String,Object> params )
    {
        formatMap( result, params, Collections.emptySet() );
    }

    static void formatMap( StringBuilder result, Map<String, Object> params, Collection<String> obfuscate )
    {
        result.append( '{' );
        if ( params != null )
        {
            String sep = "";
            for ( Map.Entry<String,Object> entry : params.entrySet() )
            {
                result
                        .append( sep )
                        .append( entry.getKey() )
                        .append( ": " );

                if ( obfuscate.contains( entry.getKey() ) )
                {
                    result.append( "******" );
                }
                else
                {
                    formatValue( result, entry.getValue() );
                }
                sep = ", ";
            }
        }
        result.append( "}" );
    }

    private static void formatValue( StringBuilder result, Object value )
    {
        if ( value instanceof Map<?,?> )
        {
            formatMapValue( result, (MapValue) value );
        }
        else if ( value instanceof String )
        {
            result.append( '\'' ).append( value ).append( '\'' );
        }
        else
        {
            result.append( Strings.prettyPrint( value ) );
        }
    }
}
