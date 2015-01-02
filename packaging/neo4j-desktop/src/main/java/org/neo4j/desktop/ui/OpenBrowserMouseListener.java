/**
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

import java.awt.Desktop;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;

import javax.swing.JLabel;

import org.neo4j.desktop.config.Environment;

import static java.awt.Cursor.DEFAULT_CURSOR;
import static java.awt.Cursor.HAND_CURSOR;
import static java.awt.Cursor.getPredefinedCursor;

/**
 * {@link MouseListener} that can open links in the systems default browser, presumably using {@link Desktop}. 
 */
public class OpenBrowserMouseListener extends MouseAdapter
{
    private final JLabel link;
    private final Environment environment;

    public OpenBrowserMouseListener( JLabel link, Environment environment )
    {
        this.link = link;
        this.environment = environment;
    }

    @Override
    public void mouseClicked( MouseEvent event )
    {
        environment.openBrowser( link.getText() );
    }

    @Override
    public void mouseEntered( MouseEvent e )
    {
        link.setCursor( getPredefinedCursor( HAND_CURSOR ) );
    }

    @Override
    public void mouseExited( MouseEvent e )
    {
        link.setCursor( getPredefinedCursor( DEFAULT_CURSOR ) );
    }
}
