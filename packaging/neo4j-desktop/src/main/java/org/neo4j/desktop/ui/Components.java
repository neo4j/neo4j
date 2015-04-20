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
import java.awt.Font;
import java.awt.LayoutManager;
import java.awt.event.ActionListener;
import java.util.Map;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.border.Border;

import org.apache.commons.lang.StringUtils;

import static java.awt.font.TextAttribute.UNDERLINE;
import static java.awt.font.TextAttribute.UNDERLINE_ON;
import static java.lang.String.format;
import static javax.swing.JOptionPane.WARNING_MESSAGE;

import static org.neo4j.desktop.ui.ScrollableOptionPane.showWrappedMessageDialog;

@SuppressWarnings("MagicConstant")
public final class Components
{
    public static final int BASE_SPACING_SIZE = 5;
    public static final int DEFAULT_TEXT_COLUMNS = 50;

    private Components()
    {
        throw new UnsupportedOperationException();
    }

    static JPanel createPanel( Component... components )
    {
        JPanel panel = new JPanel();
        for ( Component component : components )
        {
            panel.add( component );
        }
        return panel;
    }

    static JPanel withBackground( Color color, JPanel panel )
    {
        panel.setBackground( color );
        return panel;
    }

    static JPanel withLayout( LayoutManager layout, JPanel panel )
    {
        panel.setLayout( layout );
        return panel;
    }

    static JPanel withBoxLayout( int axis, JPanel panel )
    {
        return withLayout( new BoxLayout( panel, axis ), panel );
    }

    static JPanel withFlowLayout( int alignment, JPanel panel )
    {
        return withLayout( new FlowLayout( alignment ), panel );
    }

    static JPanel withFlowLayout( JPanel panel )
    {
        return withLayout( new FlowLayout(), panel );
    }

    static JPanel withSpacingBorder( JPanel panel )
    {
        return withBorder( createSpacingBorder( 1 ), panel );
    }

    static JPanel withBorder( Border border, JPanel panel )
    {
        panel.setBorder( border );
        return panel;
    }

    static Border createSpacingBorder( int size )
    {
        int inset = BASE_SPACING_SIZE * size;
        return BorderFactory.createEmptyBorder( inset, inset, inset, inset );
    }

    static JPanel withTitledBorder( String title, JPanel panel )
    {
        panel.setBorder( BorderFactory.createTitledBorder( title ) );
        return panel;
    }

    static Component createVerticalSpacing()
    {
        return Box.createVerticalStrut( BASE_SPACING_SIZE );
    }

    static JTextField createUnmodifiableTextField( String text )
    {
        return createUnmodifiableTextField( text, DEFAULT_TEXT_COLUMNS );
    }

    static JTextField createUnmodifiableTextField( String text, int columns )
    {
        JTextField textField = new JTextField( text, columns );
        textField.setEditable( false );
        textField.setForeground( Color.GRAY );
        return textField;
    }

    static JButton createTextButton( String text, ActionListener actionListener )
    {
        JButton button = new JButton( text );
        button.addActionListener( actionListener );
        return button;
    }

    public static JLabel createLabel( String... textLines )
    {
        return new JLabel( format( "<html>%s</html>", StringUtils.join( textLines, "<br>" ) ) );
    }

    static String ellipsis( String input )
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

    public static void alert( String message )
    {
        showWrappedMessageDialog( null, message, "Alert", WARNING_MESSAGE );
    }
}
