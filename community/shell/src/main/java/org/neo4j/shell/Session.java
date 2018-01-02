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
package org.neo4j.shell;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A session (or environment) for a shell client.
 */
public class Session
{
    private final Serializable id;
    private final Map<String, Object> properties = new HashMap<String, Object>();
    private final Map<String, String> aliases = new HashMap<String, String>();
    private final InterruptSignalHandler signalHandler = InterruptSignalHandler.getHandler();
    
    public Session( Serializable id )
    {
        this.id = id;
    }

    public Serializable getId()
    {
        return id;
    }

    public InterruptSignalHandler getSignalHandler()
    {
        return signalHandler;
    }

	/**
     * Sets a session value.
     * @param key the session key.
     * @param value the value.
     * @throws ShellException if the execution fails
     */
	public void set( String key, Object value ) throws ShellException
    {
	    Variables.checkIsValidVariableName( key );
        setInternal( key, value );
    }

    private void setInternal( String key, Object value )
    {
        properties.put( key, value );
    }

    /**
     * @param key the key to get the session value for.
     * @return the value for the {@code key} or {@code null} if not found.
     * @throws ShellException if the execution fails
     */
	public Object get( String key ) throws ShellException
    {
        Variables.checkIsValidVariableName( key );
        return getInternal( key );
    }

    private Object getInternal( String key )
    {
        return properties.get( key );
    }

    /**
	 * @param key the key to check the session value for.
	 * @return true if the session contains a variable with that name.
	 */
	public boolean has( String key )
	{
	    return properties.containsKey( key );
	}

	/**
     * Removes a value from the session.
     * @param key the session key to remove.
     * @return the removed value, or {@code null} if none.
     * @throws ShellException if the execution fails
     */
	public Object remove( String key ) throws ShellException
    {
        Variables.checkIsValidVariableName( key );
	    return properties.remove( key );
	}
	
	/**
	 * @return all the available session keys.
	 */
	public String[] keys()
	{
	    return properties.keySet().toArray( new String[ properties.size() ] );
	}
	
	/**
	 * Returns the session as a {@link Map} representation. Changes in the
	 * returned instance won't be reflected in the session.
	 * @return the session as a {@link Map}.
	 */
	public Map<String, Object> asMap()
	{
	    return properties;
	}

    public void removeAlias( String key )
    {
        aliases.remove( key );
    }

    public void setAlias( String key, String value )
    {
        aliases.put( key, value );
    }

    public Set<String> getAliasKeys()
    {
        return aliases.keySet();
    }

    public String getAlias( String key )
    {
        return aliases.get( key );
    }

    public void setPath( String path )
    {
        setInternal( Variables.WORKING_DIR_KEY, path );
    }

    public String getPath(  )
    {
        return (String) getInternal( Variables.WORKING_DIR_KEY );
    }

    public void setCurrent( final String value )
    {
        setInternal( Variables.CURRENT_KEY, value );
    }

    public String getCurrent()
    {
        return ( String ) getInternal( Variables.CURRENT_KEY );
    }

    public Integer getCommitCount()
    {
        return (Integer) getInternal( Variables.TX_COUNT );
    }

    public void setCommitCount( int commitCount )
    {
        setInternal( Variables.TX_COUNT, commitCount );
    }

    public String getTitleKeys() throws ShellException
    {
        return ( String ) get( Variables.TITLE_KEYS_KEY );
    }

    public String getMaxTitleLength() throws ShellException
    {
        return ( String ) get( Variables.TITLE_MAX_LENGTH );
    }
}
