/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.webadmin;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.kernel.impl.annotations.Documented;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;

public class ConsoleWebIT extends AbstractWebadminTest {

    //
    // CYPHER
    //

    /**
     * In order to access the Cypher console,
     * click on the "Console" tab in the webadmin
     * and check the "Cypher" link.
     * 
     * @@screenshot_CypherConsole
     */
    @Test
    @Documented
    public void accessing_the_Cypher_console() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Console");
        wl.clickOnLink("Cypher");
        wl.waitForElementToAppear(By
                .xpath("//ul/li[contains(.,'cypher')]"));
        captureScreenshot("screenshot_CypherConsole");
    }

   

    @Test
    public void remembersCypherStateWhenSwitchingTabsTest() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Console");
        wl.clickOnLink("Cypher");
        wl.clickOnTab("Data browser");
        wl.clickOnTab("Console");
        wl.waitForElementToAppear(By
                .xpath("//p[contains(.,'Cypher query language')]"));
    }

    @Test
    @Ignore
    public void cypherHasMultilineInput() {
        
        // Broken due to http://code.google.com/p/selenium/issues/detail?id=1723
        
        accessing_the_Cypher_console();
        
        wl.writeTo(By.id("console-input"), "start a=node(1)", Keys.RETURN);
        wl.writeTo(By.id("console-input"), "return a", Keys.RETURN);
        wl.writeTo(By.id("console-input"), Keys.RETURN);

        wl.waitForElementToAppear(By.xpath("//li[contains(.,'Node[0]')]"));
    }

    //
    // GREMLIN
    //

    @Test
    public void hasGremlinConsoleTest() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Console");
        wl.clickOnLink("Gremlin");
        wl.waitForElementToAppear(By
                .xpath("//li[contains(.,'gremlin')]"));
    }

    @Test
    @Ignore
    public void canAccessDatabaseViaGremlinConsoleTest() {
        
        // Broken due to http://code.google.com/p/selenium/issues/detail?id=1723
        
        wl.goToWebadminStartPage();
        wl.clickOnTab("Console");
        
        wl.writeTo(By.id("console-input"), "g.v(0)", Keys.RETURN);

        wl.waitForElementToAppear(By.xpath("//li[contains(.,'v[0]')]"));
    }

    //
    // HTTP CONSOLE
    //

    @Test
    public void hasHttpConsoleTest() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Console");
        wl.clickOnLink("HTTP");

        wl.waitForElementToAppear(By.xpath("//p[contains(.,'HTTP Console')]"));
    }

    @Test
    public void canAccessServerViaHttpConsoleTest() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Console");
        wl.clickOnLink("HTTP");

        wl.writeTo(By.id("console-input"), "GET /db/data/", Keys.RETURN);
        
        wl.waitForElementToAppear(By.xpath("//li[contains(.,'200')]"));
    }

    @Test
    public void httpConsoleShows404ProperlyTest() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Console");
        wl.clickOnLink("HTTP");

        wl.writeTo(By.id("console-input"), "GET /asd/ads/", Keys.RETURN);
        
        wl.waitForElementToAppear(By.xpath("//li[contains(.,'404')]"));
    }

    @Test
    public void httpConsoleShowsSyntaxErrorsTest() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Console");
        wl.clickOnLink("HTTP");

        wl.writeTo(By.id("console-input"), "blus 12 blah blah", Keys.RETURN);
        
        wl.waitForElementToAppear(By.xpath("//li[contains(.,'Invalid')]"));
    }

    @Test
    public void httpConsoleShowsJSONErrorsTest() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Console");
        wl.clickOnLink("HTTP");

        wl.writeTo(By.id("console-input"), "POST / {blah}", Keys.RETURN);
        
        wl.waitForElementToAppear(By.xpath("//li[contains(.,'Invalid')]"));
    }

}
