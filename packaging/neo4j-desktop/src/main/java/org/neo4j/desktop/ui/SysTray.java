/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.desktop.ui;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

import org.neo4j.desktop.model.SysTrayListener;

import static org.neo4j.desktop.ui.DatabaseStatus.STOPPED;
import static org.neo4j.desktop.ui.Graphics.loadImage;

public class SysTray
{
    private TrayIcon trayIcon;
    private SysTrayListener listener;
    private final String iconResourceBaseName = Graphics.SYSTEM_TRAY_ICON;

    public SysTray ( SysTrayListener listener )
    {
        this.listener = listener;

        if ( SystemTray.isSupported() )
        {
            try
            {
                init();
            }
            catch( AWTException ex )
            {
                System.err.println( ex );
            }
        }
    }

    private void init( ) throws AWTException
    {
        trayIcon = new TrayIcon( loadImage( iconResourceBaseName ), formatTitle( STOPPED ) );

        setupTrayIcon();
        SystemTray.getSystemTray().add( trayIcon );
    }

    private void setupTrayIcon()
    {
        PopupMenu popUpMenu = createPopupMenu();
        trayIcon.setPopupMenu( popUpMenu );

        trayIcon.addActionListener( new ActionListener()
        {
            @Override
            public void actionPerformed( ActionEvent e )
            {
                listener.open();
            }
        } );

        trayIcon.addMouseListener( new MouseAdapter()
        {
            @Override
            public void mouseClicked( MouseEvent e )
            {
                listener.open();
            }
        } );
    }

    private PopupMenu createPopupMenu()
    {
        PopupMenu popUpMenu = new PopupMenu();
        MenuItem menuItemOpen = new MenuItem( "Open" );
        MenuItem menuItemExit = new MenuItem( "Exit" );

        menuItemOpen.addActionListener( new ActionListener()
        {
            @Override
            public void actionPerformed( ActionEvent actionEvent )
            {
                listener.open();
            }
        } );

        menuItemExit.addActionListener( new ActionListener()
        {
            @Override
            public void actionPerformed( ActionEvent actionEvent )
            {
                listener.exit();
            }
        } );

        popUpMenu.add( menuItemOpen );
        popUpMenu.add( menuItemExit );

        return popUpMenu;
    }

    public void changeStatus( DatabaseStatus status )
    {
        trayIcon.setToolTip( formatTitle( status ) );
    }

    private String formatTitle( DatabaseStatus status )
    {
        String title = "Neo4j Community Edition";
        String formattedStatus = status.name().substring( 0, 1 ) + status.name().substring( 1 ).toLowerCase();

        return title + " - " + formattedStatus ;
    }
}
