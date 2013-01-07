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
package org.neo4j.qa.machinestate.verifier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.neo4j.qa.SharedConstants;
import org.neo4j.qa.driver.Neo4jDriver;

public class Neo4jDocumentationIsCorrect implements Verifier {

    public static Neo4jDocumentationIsCorrect neo4jDocumentationIsCorrect() {
        return new Neo4jDocumentationIsCorrect();
    }

    @Override
    public void verify(Neo4jDriver driver)
    {
        String file = driver.readFile(driver.neo4jInstallDir() + "/doc/manual/html/index.html");
        assertThat(file, containsString(SharedConstants.NEO4J_VERSION));

        List<String> files = driver.listDir(driver.neo4jInstallDir() + "/doc/manual/text");
        assertThat(files, hasItem("neo4j-manual.txt"));
    }
    
    @Override
	public String toString()
    {
    	return "The documentation exists and has correct version";
    }
}
