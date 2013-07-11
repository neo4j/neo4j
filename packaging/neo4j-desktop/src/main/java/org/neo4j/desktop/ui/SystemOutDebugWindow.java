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

import java.awt.CardLayout;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import static javax.swing.WindowConstants.HIDE_ON_CLOSE;

/**
 * Useful for debugging purposes where when created will steal the System.out/err streams and
 * make the data that gets written to them available when {@link #show() showing} the window.
 * It's an easy way to get access to that data in an environment where java is run without a console
 * to back it up.
 * 
 * Should not be used in a final released product.
 */
public class SystemOutDebugWindow
{
    private final ByteArrayOutputStream sysout = new ByteArrayOutputStream();
    private PrintStream sysoutPrinter;
    private JFrame frame;
    private JTextArea text;

    public SystemOutDebugWindow()
    {
        stealSystemOut();
        init();
    }

    private void stealSystemOut()
    {
        System.setOut( sysoutPrinter = new PrintStream( sysout ) );
        System.setErr( sysoutPrinter );
    }
    
    private void init()
    {
        frame = new JFrame( "Debug" );
        JPanel panel = new JPanel();
        panel.setLayout( new CardLayout() );
        
        sysoutPrinter.flush();
        panel.add( text = new JTextArea() );
        frame.add( new JScrollPane( panel ) );
        
        frame.pack();
        frame.setBounds( 100, 100, 600, 800 );
        frame.setVisible( false );
        frame.setDefaultCloseOperation( HIDE_ON_CLOSE );
    }

    public void show()
    {
        sysoutPrinter.flush();
        text.setText( sysout.toString() );
        frame.setVisible( true );
    }

    public void dispose()
    {
        frame.dispose();
    }
}
