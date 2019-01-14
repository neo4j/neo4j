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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IndexSpecifier
{
    private static final String GROUP_INDEX_NAME = "INAME";
    private static final String GROUP_QUOTED_INDEX_NAME = "QINAME";
    private static final String GROUP_LABEL = "LABEL";
    private static final String GROUP_QUOTED_LABEL = "QLABEL";
    private static final String GROUP_PROPERTY = "PROPERTY";
    private static final String GROUP_QUOTED_PROPERTY = "QPROPERTY";

    private static final String WHITESPACE = zeroOrMore( "\\s" );
    private static final String INITIATOR = "\\A" + WHITESPACE; // Matches beginning of input, and optional whitespace.
    private static final String TERMINATOR = WHITESPACE + "\\z"; // Matches optional whitespace, and end of input.
    private static final String CONTINUE = "\\G" + WHITESPACE; // Matches end-boundary of previous match, and optional whitespace.

    private static final String INDEX_NAME = or( identifier( GROUP_INDEX_NAME ), quotedIdentifier( GROUP_QUOTED_INDEX_NAME ) );
    private static final String LABEL = ":" + WHITESPACE + or( identifier( GROUP_LABEL ), quotedIdentifier( GROUP_QUOTED_LABEL ) );
    private static final String INDEX_OR_LABEL = or( INDEX_NAME, LABEL );
    private static final String PROPERTY_CLAUSE_BEGIN = "\\(";
    private static final String PROPERTY = or( identifier( GROUP_PROPERTY ), quotedIdentifier( GROUP_QUOTED_PROPERTY ) );
    private static final String FIRST_PROPERTY = PROPERTY;
    private static final String FOLLOWING_PROPERTY = "," + WHITESPACE + PROPERTY;
    private static final String PROPERTY_CLAUSE_END = "\\)";

    private static final Pattern PATTERN_START_INDEX_NAME_OR_LABEL = Pattern.compile( INITIATOR + INDEX_OR_LABEL ); // Initiating pattern.
    private static final Pattern PATTERN_INDEX_NAME_END = Pattern.compile( CONTINUE + TERMINATOR ); // Terminating pattern.
    private static final Pattern PATTERN_PROPERTY_CLAUSE_BEGIN = Pattern.compile( CONTINUE + PROPERTY_CLAUSE_BEGIN );
    private static final Pattern PATTERN_FIRST_PROPERTY = Pattern.compile( CONTINUE + FIRST_PROPERTY );
    private static final Pattern PATTERN_FOLLOWING_PROPERTY = Pattern.compile( CONTINUE + FOLLOWING_PROPERTY );
    private static final Pattern PATTERN_PROPERTY_CLAUSE_END = Pattern.compile( CONTINUE + PROPERTY_CLAUSE_END + TERMINATOR ); // Terminating pattern.

    private final String specification;
    private final String label;
    private final String[] properties;
    private final String name;

    public static IndexSpecifier byPatternOrName( String specification )
    {
        return parse( specification, true, true );
    }

    public static IndexSpecifier byPattern( String specification )
    {
        return parse( specification, false, true );
    }

    public static IndexSpecifier byName( String specification )
    {
        return parse( specification, true, false );
    }

    private static IndexSpecifier parse( String specification, boolean allowIndexNameSpecs, boolean allowIndexPatternSpecs )
    {
        Matcher matcher = PATTERN_START_INDEX_NAME_OR_LABEL.matcher( specification );
        if ( !matcher.find() )
        {
            throw new IllegalArgumentException( "Cannot parse index specification: '" + specification + "'" );
        }

        String indexName = either( matcher.group( GROUP_INDEX_NAME ), matcher.group( GROUP_QUOTED_INDEX_NAME ) );
        if ( indexName != null )
        {
            if ( !allowIndexNameSpecs )
            {
                throw new IllegalArgumentException( "Cannot parse index specification: '" + specification +
                        "' - it looks like an index name, which is not allowed." );
            }
            matcher.usePattern( PATTERN_INDEX_NAME_END );
            if ( matcher.find() )
            {
                return new IndexSpecifier( specification, indexName );
            }
            throw new IllegalArgumentException( "Invalid characters following index name: '" + specification + "'" );
        }

        if ( !allowIndexPatternSpecs )
        {
            throw new IllegalArgumentException( "Cannot parse index specification: '" + specification +
                    "' - it looks like an index pattern, but an index name was expected." );
        }

        String label = either( matcher.group( GROUP_LABEL ), matcher.group( GROUP_QUOTED_LABEL ) );
        if ( label == null )
        {
            throw new IllegalArgumentException( "Cannot parse index specification: '" + specification + "'" );
        }

        matcher.usePattern( PATTERN_PROPERTY_CLAUSE_BEGIN );
        if ( !matcher.find() )
        {
            throw new IllegalArgumentException( "Expected to find a property clause following the label: '" + specification + "'" );
        }

        matcher.usePattern( PATTERN_FIRST_PROPERTY );
        if ( !matcher.find() )
        {
            throw new IllegalArgumentException( "Expected to find a property in the property clause following the label: '" + specification + "'" );
        }

        List<String> properties = new ArrayList<>();
        do
        {
            String property = either( matcher.group( GROUP_PROPERTY ), matcher.group( GROUP_QUOTED_PROPERTY ) );
            if ( property == null )
            {
                throw new IllegalArgumentException( "Expected to find a property in the property clause following the label: '" + specification + "'" );
            }
            properties.add( property );
            if ( matcher.pattern() != PATTERN_FOLLOWING_PROPERTY )
            {
                matcher.usePattern( PATTERN_FOLLOWING_PROPERTY );
            }
        }
        while ( matcher.find() );

        matcher.usePattern( PATTERN_PROPERTY_CLAUSE_END );
        if ( !matcher.find() )
        {
            throw new IllegalArgumentException( "The property clause is not terminated: '" + specification + "'" );
        }

        return new IndexSpecifier( specification, label, properties.toArray( new String[0] ) );
    }

    private static String either( String first, String second )
    {
        return first != null ? first : second;
    }

    private static String or( String first, String second )
    {
        return group( group( first ) + "|" + group( second ) );
    }

    private static String identifier( String name )
    {
        return capture( name, "[A-Za-z0-9_]+" );
    }

    private static String quotedIdentifier( String name )
    {
        return group( "`" + capture( name, oneOrMore( group( or( "[^`]", "``" ) ) ) ) + "`" );
    }

    private static String group( String contents )
    {
        return "(?:" + contents + ")";
    }

    private static String capture( String name, String contents )
    {
        return "(?<" + name + ">" + contents + ")";
    }

    private static String zeroOrMore( String terms )
    {
        return terms + "*";
    }

    private static String oneOrMore( String terms )
    {
        return terms + "+";
    }

    private IndexSpecifier( String specification, String indexName )
    {
        this.specification = specification;
        this.label = null;
        this.properties = null;
        this.name = indexName;
    }

    private IndexSpecifier( String specification, String label, String[] properties )
    {
        this.specification = specification;
        this.label = label;
        this.properties = properties;
        this.name = null;
    }

    public String label()
    {
        return label;
    }

    public String[] properties()
    {
        return properties;
    }

    public String name()
    {
        return name;
    }

    @Override
    public String toString()
    {
        return specification;
    }
}
