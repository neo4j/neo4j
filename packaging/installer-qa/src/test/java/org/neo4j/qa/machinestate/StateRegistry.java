/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.qa.machinestate;

import java.util.HashMap;
import java.util.Map;

public class StateRegistry {

    private Map<Class<?>, StateAtom> state = new HashMap<Class<?>, StateAtom>();

    public void put(StateAtom atom)
    {
        state.put(atom.getClass(), atom);
    }

    public boolean contains(StateAtom atom)
    {
        return state.containsKey(atom.getClass());
    }

    public <T extends StateAtom> T get(StateAtom atom)
    {
        return get(atom.getClass());
    }

    @SuppressWarnings("unchecked")
    public <T extends StateAtom> T get(Class<? extends StateAtom> atomClass)
    {
        return (T) state.get(atomClass);
    }

    public void clear()
    {
        state.clear();
    }
    
}
