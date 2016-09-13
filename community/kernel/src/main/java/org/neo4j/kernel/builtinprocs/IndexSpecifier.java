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
package org.neo4j.kernel.builtinprocs;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.helpers.collection.Pair;

import static java.lang.String.format;

public class IndexSpecifier
{

    private final String specification;
    private final String label;
    private final String property;

    public IndexSpecifier( String specification )
    {
        this.specification = specification;
        Pair<String, String> components = parse();
        label = components.first();
        property = components.other();
    }

    public String label()
    {
        return label;
    }

    public String property()
    {
        return property;
    }

    @Override
    public String toString()
    {
        return specification;
    }

    private Pair<String, String> parse()
    {
        Pattern pattern = Pattern.compile( "" +
                ":" + or( simpleIdentifier( "simpleLabel" ), complexIdentifier( "complexLabel" ) ) +
                "\\(" + or( simpleIdentifier( "simpleProperty" ), complexIdentifier( "complexProperty" ) ) + "\\)"
        );
        Matcher matcher = pattern.matcher( specification );
        if ( !matcher.find() )
        {
            throw new IllegalArgumentException( "Cannot parse index specification " + specification );
        }
        return Pair.of(
                either( matcher.group( "simpleLabel" ), matcher.group( "complexLabel" ) ),
                either( matcher.group( "simpleProperty" ), matcher.group( "complexProperty" ) )
        );
    }

    private String either( String first, String second )
    {
        return first != null ? first : second;
    }

    private static String or( String first, String second )
    {
        return "(:?" + first + "|" + second + ")";
    }

    private static String simpleIdentifier( String name )
    {
        return format( "(?<%s>[A-Za-z0-9_]+)", name );
    }

    private static String complexIdentifier( String name )
    {
        return format( "(?:`(?<%s>[^`]+)`)", name );
    }
}
