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
package org.neo4j.qa;

import static org.neo4j.qa.machinestate.modifier.Neo4jInstallation.neo4jInstallation;
import static org.neo4j.qa.machinestate.modifier.Neo4jServiceStateModifier.neo4jRestartCommand;
import static org.neo4j.qa.machinestate.modifier.Neo4jServiceStateModifier.neo4jStartCommand;
import static org.neo4j.qa.machinestate.modifier.Neo4jServiceStateModifier.neo4jStopCommand;
import static org.neo4j.qa.machinestate.modifier.Neo4jUninstallation.neo4jUninstallation;
import static org.neo4j.qa.machinestate.modifier.VMStateModifier.vmReboot;
import static org.neo4j.qa.machinestate.verifier.Neo4jDocumentationIsCorrect.neo4jDocumentationIsCorrect;
import static org.neo4j.qa.machinestate.verifier.Neo4jRestAPIState.neo4jRestAPIDoesNotRespond;
import static org.neo4j.qa.machinestate.verifier.Neo4jRestAPIState.neo4jRestAPIResponds;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.neo4j.qa.machinestate.MachineModel;
import org.neo4j.qa.runner.StandaloneDatabaseQualityAssurance;

@RunWith(StandaloneDatabaseQualityAssurance.class)
public class BasicQualityAssuranceTest 
{

    protected final MachineModel model;
    
    public BasicQualityAssuranceTest(MachineModel model)
    {
        this.model = model;
    }

    @Test
    public void basicQualityAssurance() {
        model.apply(neo4jInstallation());
        model.verifyThat(neo4jRestAPIResponds());
        model.verifyThat(neo4jDocumentationIsCorrect());
        
        model.apply(neo4jStopCommand());
        snooze();
        model.verifyThat(neo4jRestAPIDoesNotRespond());
        
        model.apply(neo4jStartCommand());
        model.verifyThat(neo4jRestAPIResponds());
        
        model.forceApply(neo4jRestartCommand());
        model.verifyThat(neo4jRestAPIResponds());
        
        model.forceApply(vmReboot());
        
        model.verifyThat(neo4jRestAPIResponds());
        
        model.apply(neo4jUninstallation());
        model.verifyThat(neo4jRestAPIDoesNotRespond());
    }

    private void snooze() {
        int sleepyTime = 10000;
        System.out.printf("Sleeping for %d seconds.%n", sleepyTime);
        try {
            Thread.sleep(sleepyTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
