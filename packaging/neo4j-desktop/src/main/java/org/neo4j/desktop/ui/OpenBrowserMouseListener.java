/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.swing.JLabel;

public class OpenBrowserMouseListener extends MouseAdapter
{
    private final JLabel link;

    public OpenBrowserMouseListener( JLabel link )
    {
        this.link = link;
    }

    @Override
    public void mouseClicked( MouseEvent event )
    {
        Desktop desktop = Desktop.isDesktopSupported() ? Desktop.getDesktop() : null;
        if ( desktop != null && desktop.isSupported( Desktop.Action.BROWSE ) )
        {
            try
            {
                desktop.browse( new URI( link.getText() ) );
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }
            catch ( URISyntaxException e )
            {
                e.printStackTrace();
            }
        }
    }
}
