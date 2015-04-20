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
package org.neo4j.desktop.ui;

import java.awt.AWTException;
import java.awt.SystemTray;
import java.awt.TrayIcon;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import javax.swing.JFrame;

import static javax.swing.WindowConstants.DO_NOTHING_ON_CLOSE;

import static org.neo4j.desktop.ui.DatabaseStatus.STOPPED;
import static org.neo4j.desktop.ui.Graphics.loadImage;

/**
 * Adds {@link SystemTray} integration to Neo4j Desktop. Call {@link #install(org.neo4j.desktop.ui.SysTray.Actions,
 * javax.swing.JFrame)} to install it
 * where a {@link Enabled} instance will be returned if the system tray functionality is supported on this system.
 */
public abstract class SysTray
{
    public static SysTray install( Actions actions, JFrame mainWindow )
    {
        try
        {
            if ( SystemTray.isSupported() )
            {
                return new SysTray.Enabled( Graphics.SYSTEM_TRAY_ICON, actions, mainWindow );
            }
        }
        catch ( AWTException e )
        {
            // What to do here?
            e.printStackTrace( System.out );
        }
        
        // Fall back to still being able to function, but without the systray support.
        return new SysTray.Disabled( actions, mainWindow );
    }
    
    public abstract void changeStatus( DatabaseStatus status );
    
    private static class Enabled extends SysTray
    {
        private final TrayIcon trayIcon;
        private final String iconResourceBaseName;
        
        Enabled( String iconResourceBaseName, Actions actions, JFrame mainWindow ) throws AWTException
        {
            this.iconResourceBaseName = iconResourceBaseName;
            this.trayIcon = init( actions, mainWindow );
        }

        @Override
        public void changeStatus( DatabaseStatus status )
        {
            trayIcon.setImage( loadImage( tryStatusSpecific( status ) ) );
            trayIcon.setToolTip( title( status ) );
        }

        private String tryStatusSpecific( DatabaseStatus status )
        {
            String iconResource = status.name() + "-" + iconResourceBaseName;
            return SysTray.class.getResource( iconResource ) != null ? iconResource : iconResourceBaseName;
        }

        private TrayIcon init( final Actions actions, JFrame mainWindow )
                throws AWTException
        {
            TrayIcon trayIcon = new TrayIcon( loadImage( tryStatusSpecific( STOPPED ) ), title( STOPPED ) );
            trayIcon.addActionListener( new ActionListener()
            {
                @Override
                public void actionPerformed( ActionEvent e )
                {
                    actions.clickSysTray();
                }
            } );
            trayIcon.addMouseListener( new MouseAdapter()
            {
                @Override
                public void mouseClicked( MouseEvent e )
                {
                    actions.clickSysTray();
                }
            } );
            mainWindow.setDefaultCloseOperation( DO_NOTHING_ON_CLOSE );
            mainWindow.addWindowListener( new WindowAdapter()
            {
                @Override
                public void windowClosing( WindowEvent e )
                {
                    actions.clickCloseButton();
                }
            } );
            SystemTray.getSystemTray().add( trayIcon );
            return trayIcon;
        }

        private String title( DatabaseStatus status )
        {
            return "Neo4j Community (" + status.name() + ")";
        }
    }
    
    private static class Disabled extends SysTray
    {
        Disabled( final Actions actions, JFrame mainWindow )
        {
            mainWindow.setDefaultCloseOperation( DO_NOTHING_ON_CLOSE );
            mainWindow.addWindowListener( new WindowAdapter()
            {
                @Override
                public void windowClosing( WindowEvent e )
                {
                    actions.closeForReal();
                }
            } );
        }
        
        @Override
        public void changeStatus( DatabaseStatus status )
        {
            // Don't do anything.
        }
    }
    
    public interface Actions
    {
        void clickCloseButton();
        
        void clickSysTray();
        
        void closeForReal();
    }
}
