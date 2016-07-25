/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.ha.cluster.modeswitch;

import java.util.ArrayList;
import java.util.List;

/**
 * Container of {@link ComponentSwitcher}s that switches all contained components to master, slave or pending mode.
 * Assumed to be populated only during startup and is not thread safe.
 */
public class ComponentSwitcherContainer implements ComponentSwitcher
{
    // Modified during database startup while holding lock on the top level lifecycle instance
    private final List<ComponentSwitcher> componentSwitchers = new ArrayList<>();

    public void add( ComponentSwitcher componentSwitcher )
    {
        componentSwitchers.add( componentSwitcher );
    }

    @Override
    public void switchToMaster()
    {
        componentSwitchers.forEach( ComponentSwitcher::switchToMaster );
    }

    @Override
    public void switchToSlave()
    {
        componentSwitchers.forEach( ComponentSwitcher::switchToSlave );
    }

    @Override
    public void switchToPending()
    {
        componentSwitchers.forEach( ComponentSwitcher::switchToPending );
    }

    @Override
    public String toString()
    {
        return "ComponentSwitcherContainer{componentSwitchers=" + componentSwitchers + "}";
    }
}
