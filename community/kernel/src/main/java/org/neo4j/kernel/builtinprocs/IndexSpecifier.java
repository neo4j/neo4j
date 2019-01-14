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
package org.neo4j.kernel.builtinprocs;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.helpers.collection.Pair;

public class IndexSpecifier
{

    private final String specification;
    private final String label;
    private final String[] properties;

    public IndexSpecifier( String specification )
    {
        this.specification = specification;
        Pair<String,String[]> components = parse();
        label = components.first();
        properties = components.other();
    }

    public String label()
    {
        return label;
    }

    public String[] properties()
    {
        return properties;
    }

    @Override
    public String toString()
    {
        return specification;
    }

    private Pair<String,String[]> parse()
    {
        // Note that this now matches all properties in a single group, in order to split them later.
        Pattern pattern = Pattern.compile(
                ":\\s*" + or( identifier(true), qoutedIdentifier(true) ) + // Match the label
                "\\((" + or( identifier(false), qoutedIdentifier(false) ) + // Match the first property
                "(?:,\\s*" + or( identifier(false), qoutedIdentifier(false) ) + ")*)\\)" // Match following properties
        );
        Matcher matcher = pattern.matcher( specification );
        if ( !matcher.find() )
        {
            throw new IllegalArgumentException( "Cannot parse index specification " + specification );
        }
        String label = either( matcher.group( 1 ), matcher.group( 2 ) );
        String propertyString = matcher.group( 3 );
        //Split string on commas, but ignore commas in quotes
        String[] properties = propertyString.split(",\\s*(?=(?:[^`]*`[^`]*`)*[^`]*$)");
        //Strip quotes from property names
        for ( int i = 0; i < properties.length ; i++ )
        {
            properties[i] = properties[i].replaceAll( "(^`)|(`$)", "" );
        }
        return Pair.of( label, properties );
    }

    private String either( String first, String second )
    {
        return first != null ? first : second;
    }

    private static String or( String first, String second )
    {
        return "(?:" + first + "|" + second + ")";
    }

    private static String identifier( boolean capture )
    {
        if ( capture )
        {
            return "([A-Za-z0-9_]+)";
        }
        else
        {
            return "(?:[A-Za-z0-9_]+)";
        }
    }

    private static String qoutedIdentifier( boolean capture )
    {
        if ( capture )
        {
            return "(?:`((?:[^`]|``)+)`)";
        }
        else
        {
            return "(?:`(?:(?:[^`]|``)+)`)";
        }
    }
}
