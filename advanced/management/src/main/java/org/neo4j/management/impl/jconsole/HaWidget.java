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
package org.neo4j.management.impl.jconsole;

import java.util.Collection;

import javax.swing.JLabel;
import javax.swing.JPanel;

import org.neo4j.management.HighAvailability;

class HaWidget extends Widget
{
    HaWidget( ManagementAccess manager, HighAvailability ha )
    {
        // TODO Auto-generated constructor stub
    }

    @Override
    void populate( JPanel view )
    {
        view.add( new JLabel( "Neo4j is Highly Available!" ) );
    }

    @Override
    void update( Collection<UpdateEvent> result )
    {
        // TODO tobias: Implement update() [Nov 30, 2010]
    }
}
