/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.event;

/**
 * This class represents the {@link EventManager} module. It receives lifecycle
 * events from the module framework and supports configuration of the
 * event module.
 */
public class EventModule
{
	private static final String	MODULE_NAME			= "EventModule";
	
	public void init()
	{
	}
	
	public void start()
	{
		EventManager.getManager().start();
	}
	
	public void reload()
	{
		EventManager.getManager().stop();
		EventManager.getManager().start();
	}
	
	public void stop()
	{
		EventManager.getManager().stop();
	}
	
	public void destroy()
	{
		EventManager.getManager().destroy();
	}
	
	public String getModuleName()
	{
		return MODULE_NAME;
	}
	
	public void setReActiveEventQueueWaitTime( int time )
	{
		EventManager.getManager().setReActiveEventQueueWaitTime( time );
	}
	
	public int getReActiveEventQueueWaitTime()
	{
		return EventManager.getManager().getReActiveEventQueueWaitTime();
	}
	
	public void setReActiveEventQueueNotifyOnCount( int count )
	{
		EventManager.getManager().setReActiveEventQueueNotifyOnCount( count );
	}
	
	public int getReActiveEventQueueNotifyOnCount()
	{
		return EventManager.getManager().getReActiveEventQueueNotifyOnCount();
	}
}