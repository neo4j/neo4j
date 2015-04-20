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

import java.util.ArrayList;
import java.util.Collection;

import javax.swing.JPanel;

abstract class Widget
{
    final JPanel view()
    {
        JPanel panel = new JPanel();
        populate( panel );
        return panel;
    }

    abstract void populate( JPanel view );

    void dispose()
    {
    }

    final UpdateEvent[] update()
    {
        Collection<UpdateEvent> result = new ArrayList<UpdateEvent>();
        update( result );
        return result.isEmpty() ? null : result.toArray( new UpdateEvent[result.size()] );
    }

    abstract void update( Collection<UpdateEvent> result );
}
