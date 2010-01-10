/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.shell.impl;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.shell.App;
import org.neo4j.shell.AppShellServer;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.util.json.JSONArray;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;

/**
 * Common abstract implementation of an {@link App}.
 */
public abstract class AbstractApp implements App
{
	private Map<String, OptionContext> typeExceptions =
		new HashMap<String, OptionContext>();
	private AppShellServer server;
	
	public String getName()
	{
		return this.getClass().getSimpleName().toLowerCase();
	}

	public OptionValueType getOptionValueType( String option )
	{
		OptionContext context = this.typeExceptions.get( option );
		return context == null ? OptionValueType.NONE : context.getType();
	}

	protected void addValueType( String option, OptionContext context )
	{
		this.typeExceptions.put( option, context );
	}
	
	public String[] getAvailableOptions()
	{
		String[] result = this.typeExceptions.keySet().toArray(
			new String[ this.typeExceptions.size() ] );
		Arrays.sort( result );
		return result;
	}

	public void setServer( AppShellServer server )
	{
		this.server = server;
	}
	
	protected AppShellServer getServer()
	{
		return this.server;
	}
	
	public String getDescription()
	{
		return null;
	}
	
	public String getDescription( String option )
	{
		OptionContext context = this.typeExceptions.get( option );
		return context == null ? null : context.getDescription();
	}

	protected void printMany( Output out, String string, int count )
		throws RemoteException
	{
		for ( int i = 0; i < count; i++ )
		{
			out.print( string );
		}
	}

	protected static Serializable safeGet( Session session, String key )
	{
		try
		{
			return session.get( key );
		}
		catch ( RemoteException e )
		{
			throw new RuntimeException( e );
		}
	}
	
	protected static void safeSet( Session session, String key,
		Serializable value )
	{
		try
		{
			session.set( key, value );
		}
		catch ( RemoteException e )
		{
			throw new RuntimeException( e );
		}
	}

	protected static Serializable safeRemove( Session session, String key )
	{
		try
		{
			return session.remove( key );
		}
		catch ( RemoteException e )
		{
			throw new RuntimeException( e );
		}
	}
	
	protected static Map<String, Object> parseFilter( String filterString,
	    Output out ) throws RemoteException
	{
	    if ( filterString == null )
	    {
	        return new HashMap<String, Object>();
	    }
	    
	    Map<String, Object> map = null;
	    String signsOfJSON = ":";
	    int numberOfSigns = 0;
	    for ( int i = 0; i < signsOfJSON.length(); i++ )
	    {
	        if ( filterString.contains(
	            String.valueOf( signsOfJSON.charAt( i ) ) ) )
	        {
	            numberOfSigns++;
	        }
	    }
	    
	    if ( numberOfSigns >= 1 )
	    {
            String jsonString = filterString;
            if ( !jsonString.startsWith( "{" ) )
            {
                jsonString = "{" + jsonString;
            }
            if ( !jsonString.endsWith( "}" ) )
            {
                jsonString += "}";
            }
            try
            {
                map = parseJSONMap( jsonString );
            }
            catch ( JSONException e )
            {
                out.println( "parser: \"" + filterString + "\" hasn't got " +
                	"correct JSON formatting: " + e.getMessage() );
            }
	    }
	    else
	    {
	        map = new HashMap<String, Object>();
	        map.put( filterString, null );
	    }
	    return map;
	}

    private static Map<String, Object> parseJSONMap( String jsonString )
        throws JSONException
	{
        JSONObject object = new JSONObject( jsonString );
        Map<String, Object> result = new HashMap<String, Object>();
        for ( String name : JSONObject.getNames( object ) )
        {
            Object value = object.get( name );
            if ( value != null && value instanceof String &&
                ( ( String ) value ).length() == 0 )
            {
                value = null;
            }
            result.put( name, value );
        }
        return result;
	}
    
    protected static Object[] parseArray( String string )
    {
        try
        {
            JSONArray array = new JSONArray( string );
            Object[] result = new Object[ array.length() ];
            for ( int i = 0; i < result.length; i++ )
            {
                result[ i ] = array.get( i );
            }
            return result;
        }
        catch ( JSONException e )
        {
            throw new IllegalArgumentException( e.getMessage(), e );
        }
    }
    
	/**
	 * Groups an {@link OptionValueType} and a description.
	 */
	public static class OptionContext
	{
		private OptionValueType type;
		private String description;
		
		/**
		 * @param type the type for the option.
		 * @param description the description of the option.
		 */
		public OptionContext( OptionValueType type, String description )
		{
			this.type = type;
			this.description = description;
		}
		
		/**
		 * @return the option value type.
		 */
		public OptionValueType getType()
		{
			return this.type;
		}
		
		/**
		 * @return the description.
		 */
		public String getDescription()
		{
			return this.description;
		}
	}
}
