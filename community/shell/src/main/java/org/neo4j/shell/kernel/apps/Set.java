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
package org.neo4j.shell.kernel.apps;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.Service;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

/**
 * Sets a property for the current node or relationship.
 */
@Service.Implementation( App.class )
public class Set extends TransactionProvidingApp
{
    private static class ValueTypeContext
    {
        private final Class<?> fundamentalClass;
        private final Class<?> boxClass;
        private final Class<?> fundamentalArrayClass;
        private final Class<?> boxArrayClass;

        ValueTypeContext( Class<?> fundamentalClass, Class<?> boxClass,
            Class<?> fundamentalArrayClass, Class<?> boxArrayClass )
        {
            this.fundamentalClass = fundamentalClass;
            this.boxClass = boxClass;
            this.fundamentalArrayClass = fundamentalArrayClass;
            this.boxArrayClass = boxArrayClass;
        }

        public String getName()
        {
            return fundamentalClass.equals( boxClass ) ?
                boxClass.getSimpleName() : fundamentalClass.getSimpleName();
        }

        public String getArrayName()
        {
            return getName() + "[]";
        }
    }

    private static class ValueType
    {
        private final ValueTypeContext context;
        private final boolean isArray;

        ValueType( ValueTypeContext context, boolean isArray )
        {
            this.context = context;
            this.isArray = isArray;
        }
    }

    private static final Map<String, ValueType> NAME_TO_VALUE_TYPE =
        new HashMap<String, ValueType>();
    static
    {
        mapNameToValueType( new ValueTypeContext( boolean.class,
            Boolean.class, boolean[].class, Boolean[].class ) );
        mapNameToValueType( new ValueTypeContext(
            byte.class, Byte.class, byte[].class, Byte[].class ) );
        mapNameToValueType( new ValueTypeContext( char.class,
            Character.class, char[].class, Character[].class ) );
        mapNameToValueType( new ValueTypeContext(
            short.class, Short.class, short[].class, Short[].class ) );
        mapNameToValueType( new ValueTypeContext(
            int.class, Integer.class, int[].class, Integer[].class ) );
        mapNameToValueType( new ValueTypeContext(
            long.class, Long.class, long[].class, Long[].class ) );
        mapNameToValueType( new ValueTypeContext(
            float.class, Float.class, float[].class, Float[].class ) );
        mapNameToValueType( new ValueTypeContext(
            double.class, Double.class, double[].class, Double[].class ) );
        mapNameToValueType( new ValueTypeContext(
            String.class, String.class, String[].class, String[].class ) );
    }

    private static final Map<Class<?>, String> VALUE_TYPE_TO_NAME =
        new HashMap<Class<?>, String>();
    static
    {
        for ( Map.Entry<String, ValueType> entry :
            NAME_TO_VALUE_TYPE.entrySet() )
        {
            ValueTypeContext context = entry.getValue().context;
            VALUE_TYPE_TO_NAME.put( context.fundamentalClass,
                context.getName() );
            VALUE_TYPE_TO_NAME.put( context.boxClass,
                context.getName() );
            VALUE_TYPE_TO_NAME.put( context.fundamentalArrayClass,
                context.getArrayName() );
            VALUE_TYPE_TO_NAME.put( context.boxArrayClass,
                context.getArrayName() );
        }
    }

    private static void mapNameToValueType( ValueTypeContext context )
    {
        NAME_TO_VALUE_TYPE.put( context.getName(),
            new ValueType( context, false ) );
        NAME_TO_VALUE_TYPE.put( context.getArrayName(),
            new ValueType( context, true ) );
    }

    /**
     * Constructs a new "set" application.
     */
    public Set()
    {
        super();
        this.addOptionDefinition( "t", new OptionDefinition( OptionValueType.MUST,
            "Value type, f.ex: String, String[], int, long[], byte a.s.o. " +
            "If an array type is supplied the value(s) are given in a " +
            "JSON-style array format, f.ex:\n" +
            "[321,45324] for an int[] or\n" +
            "\"['The first string','The second string here']\" for a " +
            "String[]" ) );
        this.addOptionDefinition( "p", new OptionDefinition( OptionValueType.NONE,
            "Tells the command to set the supplied values as property." ) );
        this.addOptionDefinition( "l", new OptionDefinition( OptionValueType.MUST,
                "Sets one or more labels on the current node." ) );
    }

    @Override
    public String getDescription()
    {
        return "Sets a property on the current node or relationship or label on the current node.\n" +
        		"Usage:\n" +
        		"  set <key> <value>\n" +
        		"  set -p <key> <value>\n" +
        		"  set -l PERSON";
    }

    protected static String getValueTypeName( Class<?> cls )
    {
        return VALUE_TYPE_TO_NAME.get( cls );
    }

    @Override
    protected Continuation exec( AppCommandParser parser, Session session,
        Output out ) throws ShellException
    {
        boolean forProperty = parser.options().containsKey( "p" );
        boolean forLabel = parser.options().containsKey( "l" );
        if ( forProperty || !forLabel )
        {   // Property
            if ( parser.arguments().size() < 2 )
            {
                throw new ShellException( "Must supply key and value, " +
                    "like: set title \"This is a my title\"" );
            }

            String key = parser.arguments().get( 0 );
            ValueType valueType = getValueType( parser );
            Object value = parseValue( parser.arguments().get( 1 ), valueType );

            NodeOrRelationship thing = getCurrent( session );
            thing.setProperty( key, value );
        }
        else
        {   // Label
            Node node = getCurrent( session ).asNode();
            for ( Label label : parseLabels( parser ) )
            {
                node.addLabel( label );
            }
        }
        
        return Continuation.INPUT_COMPLETE;
    }

    private static Object parseValue( String stringValue, ValueType valueType )
    {
        Object result = null;
        if ( valueType.isArray )
        {
            Class<?> componentType = valueType.context.boxClass;
            Object[] rawArray = parseArray( stringValue );
            result = Array.newInstance( componentType, rawArray.length );
            for ( int i = 0; i < rawArray.length; i++ )
            {
                Array.set( result, i,
                    parseValue( rawArray[ i ].toString(), componentType ) );
            }
        }
        else
        {
            Class<?> componentType = valueType.context.boxClass;
            result = parseValue( stringValue, componentType );
        }
        return result;
    }

    private static Object parseValue( String value, Class<?> type )
    {
        // TODO Are you tellin' me this can't be done in a better way?
        Object result = null;
        if ( type.equals( String.class ) )
        {
            result = value;
        }
        else if ( type.equals( Boolean.class ) )
        {
            result = Boolean.parseBoolean( value );
        }
        else if ( type.equals( Byte.class ) )
        {
            result = Byte.parseByte( value );
        }
        else if ( type.equals( Character.class ) )
        {
            result = value.charAt( 0 );
        }
        else if ( type.equals( Short.class ) )
        {
            result = Short.parseShort( value );
        }
        else if ( type.equals( Integer.class ) )
        {
            result = Integer.parseInt( value );
        }
        else if ( type.equals( Long.class ) )
        {
            result = Long.parseLong( value );
        }
        else if ( type.equals( Float.class ) )
        {
            result = Float.parseFloat( value );
        }
        else if ( type.equals( Double.class ) )
        {
            result = Double.parseDouble( value );
        }
        else
        {
            throw new IllegalArgumentException( "Invalid type " + type );
        }
        return result;
    }

    private static ValueType getValueType( AppCommandParser parser )
        throws ShellException
    {
        String type = parser.options().containsKey( "t" ) ?
            parser.options().get( "t" ) : String.class.getSimpleName();
        ValueType valueType = NAME_TO_VALUE_TYPE.get( type );

        if ( valueType == null )
        {
            throw new ShellException( "Invalid value type '" + type + "'" );
        }
        return valueType;
    }
}