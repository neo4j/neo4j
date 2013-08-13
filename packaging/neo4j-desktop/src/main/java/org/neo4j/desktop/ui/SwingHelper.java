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

import java.awt.Font;
import java.awt.Image;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.Map;
import javax.imageio.ImageIO;
import javax.swing.JButton;

import static java.awt.font.TextAttribute.UNDERLINE;
import static java.awt.font.TextAttribute.UNDERLINE_ON;
import static java.lang.String.format;

public class SwingHelper
{
    public static Image loadImage( String resource )
    {
        try
        {
            return ImageIO.read( SwingHelper.class.getResource( resource ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    private SwingHelper()
    {
    }

    static JButton buttonWithText( String text, ActionListener actionListener )
    {
        JButton button = new JButton( text );
        button.addActionListener( actionListener );
        return button;
    }

    static String elipsis( String input )
    {
        return format( "%s\u2026", input );
    }

    @SuppressWarnings( { "unchecked", "rawtypes" } )
    static Font underlined( Font font )
    {
        Map attributes = font.getAttributes();
        attributes.put( UNDERLINE, UNDERLINE_ON );
        return font.deriveFont( attributes );
    }
}
