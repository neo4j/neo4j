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

import java.awt.FlowLayout;

import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSeparator;

import static javax.swing.BoxLayout.Y_AXIS;

public class HeadlinePanel extends JPanel
{
    private static final long serialVersionUID = 1L;

    public HeadlinePanel( String headline )
    {
        super();
        this.setLayout( new BoxLayout( this, Y_AXIS ) );
        this.add( new JSeparator() );
        JPanel labelPanel = new JPanel();
        labelPanel.setLayout( new FlowLayout() );
        labelPanel.add( new JLabel( headline ) );
        this.add( labelPanel );
    }
}
