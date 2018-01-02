/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.visualization.graphviz;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.ArrayIterator;
import org.neo4j.helpers.collection.CombiningIterator;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.PrefetchingIterator;

public class ConfigurationParser
{
    @SuppressWarnings( "unchecked" )
    public ConfigurationParser( File configFile, String... format )
    {
        this( IteratorUtil.asIterable( new CombiningIterator<String>( Arrays.asList(
                new LineIterator( configFile ), new ArrayIterator<String>( format ) ) ) ) );
    }

    public ConfigurationParser( String... format )
    {
        this( IteratorUtil.asIterable( new ArrayIterator<String>( format ) ) );
    }

    public ConfigurationParser( Iterable<String> format )
    {
        Class<? extends ConfigurationParser> type = getClass();
        for ( String spec : format )
        {
            String[] parts = spec.split( "=", 2 );
            String name = parts[0];
            String[] args = null;
            Method method;
            Throwable error = null;
            try
            {
                if ( parts.length == 1 )
                {
                    method = type.getMethod( name, String[].class );
                }
                else
                {
                    try
                    {
                        method = type.getMethod( name, String.class );
                        args = new String[] { parts[1] };
                    }
                    catch ( NoSuchMethodException nsm )
                    {
                        error = nsm; // use as a flag to know how to invoke
                        method = type.getMethod( name, String[].class );
                        args = parts[1].split( "," );
                    }
                }
                try
                {
                    if ( error == null )
                    {
                        method.invoke( this, (Object[]) args );
                    }
                    else
                    {
                        error = null; // reset the flag use
                        method.invoke( this, (Object) args );
                    }
                }
                catch ( InvocationTargetException ex )
                {
                    error = ex.getTargetException();
                    if ( error instanceof RuntimeException )
                    {
                        throw (RuntimeException) error;
                    }
                }
                catch ( Exception ex )
                {
                    error = ex;
                }
            }
            catch ( NoSuchMethodException nsm )
            {
                error = nsm;
            }
            if ( error != null )
            {
                throw new IllegalArgumentException( "Unknown parameter \""
                                                    + name + "\"", error );
            }
        }
    }

    private final List<StyleParameter> styles = new ArrayList<StyleParameter>();

    public final StyleParameter[] styles( StyleParameter... params )
    {
        if ( params == null ) params = new StyleParameter[0];
        StyleParameter[] result = styles.toArray( new StyleParameter[styles.size() + params.length] );
        System.arraycopy( params, 0, result, styles.size(), params.length );
        return result;
    }

    public void nodeTitle( String pattern )
    {
        final PatternParser parser = new PatternParser( pattern );
        styles.add( new StyleParameter.NodeTitle()
        {
            public String getTitle( Node container )
            {
                return parser.parse( container );
            }
        } );
    }

    public void relationshipTitle( String pattern )
    {
        final PatternParser parser = new PatternParser( pattern );
        styles.add( new StyleParameter.RelationshipTitle()
        {
            public String getTitle( Relationship container )
            {
                return parser.parse( container );
            }
        } );
    }

    public void nodePropertyFilter( String nodeProperties )
    {
        final String nodePropertiesString = nodeProperties;
        styles.add( new StyleParameter.NodePropertyFilter() {
          public boolean acceptProperty(String key) {
            return Arrays.asList(nodePropertiesString.split(",")).contains(key);
          }
        });
    }

    public void reverseOrder( String... typeNames )
    {
        if (typeNames== null || typeNames.length == 0) return;
        RelationshipType[] types = new RelationshipType[typeNames.length];
        for ( int i = 0; i < typeNames.length; i++ )
        {
            types[i] = DynamicRelationshipType.withName( typeNames[i] );
        }
        styles.add( new StyleParameter.ReverseOrderRelationshipTypes( types ) );
    }

    private static class PatternParser
    {
        private final String pattern;

        PatternParser( String pattern )
        {
            this.pattern = pattern;

        }

        String parse( PropertyContainer container )
        {
            StringBuilder result = new StringBuilder();
            for ( int pos = 0; pos < pattern.length(); )
            {
                char cur = pattern.charAt( pos++ );
                if ( cur == '@' )
                {
                    String key = untilNonAlfa( pos );
                    result.append( getSpecial( key, container ) );
                    pos += key.length();
                }
                else if ( cur == '$' )
                {
                    String key;
                    if ( pattern.charAt( pos ) == '{' )
                    {
                        key = pattern.substring( ++pos, pattern.indexOf( '}',
                                pos++ ) );
                    }
                    else
                    {
                        key = untilNonAlfa( pos );
                    }
                    pos += pattern.length();
                    result.append( container.getProperty( key ) );
                }
                else if ( cur == '\\' )
                {
                    result.append( pattern.charAt( pos++ ) );
                }
                else
                {
                    result.append( cur );
                }
            }
            return result.toString();
        }

        private String untilNonAlfa( int start )
        {
            int end = start;
            while ( end < pattern.length() && Character.isLetter( pattern.charAt( end ) ) )
            {
                end++;
            }
            return pattern.substring( start, end );
        }

        private String getSpecial( String attribute, PropertyContainer container )
        {
            if ( attribute.equals( "id" ) )
            {
                if ( container instanceof Node )
                {
                    return "" + ( (Node) container ).getId();
                }
                else if ( container instanceof Relationship )
                {
                    return "" + ( (Relationship) container ).getId();
                }
            }
            else if ( attribute.equals( "type" ) )
            {
                if ( container instanceof Relationship )
                {
                    return ( (Relationship) container ).getType().name();
                }
            }
            return "@" + attribute;
        }
    }

    private static class LineIterator extends PrefetchingIterator<String>
    {
        private final BufferedReader reader;

        public LineIterator( BufferedReader reader )
        {
            this.reader = reader;
        }

        public LineIterator( File file )
        {
            this( fileReader( file ) );
        }

        private static BufferedReader fileReader( File file )
        {
            try
            {
                return new BufferedReader( new InputStreamReader( new FileInputStream( file ), "UTF-8" ) );
            }
            catch ( FileNotFoundException e )
            {
                return null;
            }
            catch ( UnsupportedEncodingException e )
            {
                throw new RuntimeException( e );
            }
        }

        @Override
        protected String fetchNextOrNull()
        {
            try
            {
                return reader.readLine();
            }
            catch ( Exception e )
            {
                return null;
            }
        }
    }
}
