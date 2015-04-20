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

import java.awt.Color;
import java.awt.Component;
import java.awt.FlowLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;

import static org.neo4j.desktop.ui.Components.createPanel;
import static org.neo4j.desktop.ui.Components.ellipsis;
import static org.neo4j.desktop.ui.Components.withBackground;
import static org.neo4j.desktop.ui.Components.withLayout;

public enum DatabaseStatus
{
    STOPPED,
    STARTING,
    STARTED,
    STOPPING;
    public static final Color STOPPED_COLOR = new Color( 1.0f, 0.5f, 0.5f );
    public static final Color CHANGING_COLOR = new Color( 1.0f, 1.0f, 0.5f );
    public static final Color STARTED_COLOR = new Color( 0.5f, 1.0f, 0.5f );

    public Component display( DesktopModel model )
    {
        switch ( this )
        {
            case STOPPED:
                return createTextStatusDisplay( STOPPED_COLOR, "Choose a graph database directory, " +
                        "then start the server" );
            case STARTING:
                return createTextStatusDisplay( CHANGING_COLOR, ellipsis( "In just a few seconds, Neo4j will be ready" ) );
            case STARTED:
                return createStartedStatusDisplay( model );
            case STOPPING:
                return createTextStatusDisplay( CHANGING_COLOR, ellipsis( "Neo4j is shutting down" ) );
            default:
                throw new IllegalStateException();
        }
    }

    private static JPanel createTextStatusDisplay( Color color, String text )
    {
        return createStatusDisplay( color, new JLabel( text ) );
    }

    private static JPanel createStartedStatusDisplay( DesktopModel model )
    {
        final JLabel link = new JLabel( "http://localhost:7474/" );

        model.register( new DesktopModelListener() {
            @Override
            public void desktopModelChanged(DesktopModel model) {
                link.setText("http://localhost:" + model.getServerPort() +  "/");
            }
        });

        link.setFont( Components.underlined( link.getFont() ) );
        link.addMouseListener( new OpenBrowserMouseListener( link, model ) );

        return createStatusDisplay( STARTED_COLOR, new JLabel( "Neo4j is ready. Browse to " ), link );
    }

    private static JPanel createStatusDisplay( Color color, Component... components )
    {
        return withBackground( color, withLayout( new FlowLayout(), createPanel( components ) ) );
    }
}
