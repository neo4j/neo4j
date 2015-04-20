/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.management.impl.jconsole;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.swing.JPanel;
import javax.swing.SwingWorker;

import org.neo4j.management.HighAvailability;
import org.neo4j.management.RemoteConnection;

import com.sun.tools.jconsole.JConsolePlugin;

/**
 * Neo4j Plugin for JConsole.
 *
 * @author Tobias Ivarsson <tobias.ivarsson@neotechnology.com>
 */
public class Neo4jPlugin extends JConsolePlugin
{
    private final Collection<Widget> widgets = new LinkedList<Widget>();

    @Override
    public Map<String, JPanel> getTabs()
    {
        ManagementAccess[] managers = ManagementAccess.getAll( getContext().getMBeanServerConnection() );
        Map<String, JPanel> result = new LinkedHashMap<String, JPanel>();
        if ( managers.length == 1 )
        {
            addTabs( managers[0], "", result );
        }
        else
        {
            for ( ManagementAccess manager : managers )
            {
                addTabs( manager,
                        " (" + manager.getMBeanQuery().getKeyProperty( "instance" ) + ")", result );
            }
        }
        return result;
    }

    private void addTabs( ManagementAccess manager, String suffix, Map<String, JPanel> result )
    {
        result.put( "Neo4j" + suffix, add( new KernelWidget( manager ) ) );
        try
        {
            HighAvailability ha = manager.getBean( HighAvailability.class );
            if ( ha != null )
            {
                result.put( "Neo4j HA" + suffix, add( new HaWidget( manager, ha ) ) );
            }
        }
        catch ( Exception haNotAvailable )
        {
            // ok, just don't include HA info
        }
        try
        {
            RemoteConnection remote = manager.getBean( RemoteConnection.class );
            if ( remote != null )
            {
                DataBrowser browser = new DataBrowser( remote );
                result.put( "Neo4j Graph" + suffix, browser.view() );
            }
        }
        catch ( LinkageError dataBrowserNotAvailable )
        {
            // ok, just don't data browser
        }
        catch ( Exception dataBrowserNotAvailable )
        {
            // ok, just don't data browser
        }
    }

    private JPanel add( Widget tab )
    {
        widgets.add( tab );
        return tab.view();
    }

    @Override
    public void dispose()
    {
        for ( Widget widget : widgets )
        {
            widget.dispose();
        }
        super.dispose();
    }

    @Override
    public SwingWorker<?, ?> newSwingWorker()
    {
        return new SwingWorker<Void, UpdateEvent>()
        {
            @Override
            protected Void doInBackground() throws Exception
            {
                Iterator<Widget> widget = widgets.iterator();
                for ( int i = 0; widget.hasNext(); i++ )
                {
                    setProgress( ( 100 * i ) / widgets.size() );
                    UpdateEvent[] updates = widget.next().update();
                    if ( updates != null ) publish( updates );
                }
                setProgress( 100 );
                return null;
            }

            @Override
            protected void process( List<UpdateEvent> events )
            {
                for ( UpdateEvent event : events )
                {
                    event.apply();
                }
            }
        };
    }
}
