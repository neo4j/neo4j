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

import java.io.File;

import org.neo4j.qa.driver.Neo4jDriver;
import org.neo4j.qa.machinestate.StateAtom;
import org.neo4j.qa.machinestate.StateRegistry;

public class DownloadLogs implements MachineModifier {

    private final String targetFolder;

    public DownloadLogs(String targetFolder)
    {
        this.targetFolder = targetFolder;
    }

    @Override
    public void modify(Neo4jDriver driver, StateRegistry state)
    {
        new File(targetFolder).mkdirs();
        try {
            driver.downloadLogsTo(targetFolder);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public StateAtom[] stateModifications()
    {
        return new StateAtom[] {};
    }

}
