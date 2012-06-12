/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.qa.machinestate.modifier;

import org.neo4j.qa.driver.Neo4jDriver;
import org.neo4j.qa.machinestate.Neo4jServiceState;
import org.neo4j.qa.machinestate.StateAtom;
import org.neo4j.qa.machinestate.StateRegistry;

public class Neo4jServiceStateModifier implements MachineModifier {

    private enum Command {
        START,
        STOP,
        RESTART
    }
    
    public static Neo4jServiceStateModifier neo4jStartCommand() {
        return new Neo4jServiceStateModifier(Command.START);
    }
    
    public static Neo4jServiceStateModifier neo4jRestartCommand() {
        return new Neo4jServiceStateModifier(Command.RESTART);
    }

    public static Neo4jServiceStateModifier neo4jStopCommand() {
        return new Neo4jServiceStateModifier(Command.STOP);
    }
    
    private final Command command;

    public Neo4jServiceStateModifier(Command command)
    {
        this.command = command;
    }

    @Override
    public void modify(Neo4jDriver driver, StateRegistry state)
    {
        switch(command) {
        case START:
            driver.startNeo4j();
            break;
        case STOP:
            driver.stopNeo4j();
            break;
        case RESTART:
            driver.stopNeo4j();
            driver.startNeo4j();
            break;
        }
    }

    @Override
    public StateAtom[] stateModifications()
    {
        switch(command) {
        case START:
            return new StateAtom[]{Neo4jServiceState.RUNNING};
        case STOP:
            return new StateAtom[]{Neo4jServiceState.STOPPED};
        case RESTART:
            return new StateAtom[]{Neo4jServiceState.RUNNING};
        }
        return new StateAtom[]{};
    }
    
}
