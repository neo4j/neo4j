/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
