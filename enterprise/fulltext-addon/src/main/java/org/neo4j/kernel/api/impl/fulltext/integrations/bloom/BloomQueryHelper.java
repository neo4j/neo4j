/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.fulltext.integrations.bloom;

import org.apache.lucene.queryparser.classic.QueryParser;

import java.util.Collection;

import static java.util.stream.Collectors.joining;

public class BloomQueryHelper
{
    public static String createQuery( Collection<String> query, boolean fuzzy, boolean matchAll )
    {
        String delim = "";
        if ( matchAll )
        {
            delim = "&& ";
        }
        if ( fuzzy )
        {
            return query.stream().map( QueryParser::escape ).collect( joining( "~ " + delim, "", "~" ) );
        }
        return query.stream().map( QueryParser::escape ).collect( joining( " " + delim ) );
    }
}
