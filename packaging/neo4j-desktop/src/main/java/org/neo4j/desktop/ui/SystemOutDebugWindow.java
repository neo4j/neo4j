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

import java.awt.CardLayout;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import static javax.swing.WindowConstants.HIDE_ON_CLOSE;

/**
 * Useful for debugging purposes: an easy way to get access to that data in an environment where java is run without
 * a console.
 *
 * When created will tee the System.out/err streams and pipe them into an <b>unbounded</b> byte array.
 */
public class SystemOutDebugWindow
{
    private static final int START_X = 100;
    private static final int START_Y = 100;
    private static final int START_WIDTH = 600;
    private static final int START_HEIGHT = 800;

    private final ByteArrayOutputStream sysStreamCapture = new ByteArrayOutputStream();
    private PrintStream sysStreamPrinter;
    private JFrame frame;
    private JTextArea text;

    public SystemOutDebugWindow()
    {
        stealSystemOut();
        init();
    }

    private void stealSystemOut()
    {
        sysStreamPrinter = new PrintStream( new TeeOutputStream( System.out, sysStreamCapture ) );
        System.setOut( sysStreamPrinter );
        System.setErr( sysStreamPrinter );
    }
    
    private void init()
    {
        frame = new JFrame( "Debug" );
        JPanel panel = new JPanel();
        panel.setLayout( new CardLayout() );

        sysStreamPrinter.flush();
        text = new JTextArea();
        panel.add( "status", text );
        frame.add( new JScrollPane( panel ) );
        
        frame.pack();
        frame.setBounds( START_X, START_Y, START_WIDTH, START_HEIGHT );
        frame.setVisible( false );
        frame.setDefaultCloseOperation( HIDE_ON_CLOSE );
    }

    public void show()
    {
        sysStreamPrinter.flush();
        text.setText( sysStreamCapture.toString() );
        frame.setVisible( true );
    }

    public void dispose()
    {
        frame.dispose();
    }
}
