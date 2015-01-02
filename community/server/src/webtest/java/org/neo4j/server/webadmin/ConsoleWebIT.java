/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

@Ignore("On avengers wall to be resolved")
public class ConsoleWebIT extends AbstractWebadminTest {

    //
    // SHELL
    //

    /**
     * In order to access the Shell console,
     * click on the "Console" tab in the webadmin
     * and check the "Shell" link.
     * 
     * @@screenshot_ShellConsole
     */
    @Test
    @Documented
    public void accessing_the_Shell_console() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Console");
        wl.clickOnLink("Neo4j Shell");
        wl.waitForElementToAppear(By.xpath("//ul/li[contains(.,'neo4j-sh')]"));
        captureScreenshot("screenshot_ShellConsole");
    }

    @Test
    public void remembersShellStateWhenSwitchingTabsTest() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Console");
        wl.clickOnLink("HTTP");
        wl.clickOnTab("Data browser");
        wl.clickOnTab("Console");
        wl.waitForElementToAppear(By
                .xpath("//li[contains(.,'200')]"));
    }

    @Test
    @Ignore( "Broken due to http://code.google.com/p/selenium/issues/detail?id=1723" )
    public void cypherHasMultilineInput()
    {
        accessing_the_Shell_console();
        
        wl.writeTo(By.id("console-input"), "start a=node(1)", Keys.RETURN);
        wl.writeTo(By.id("console-input"), "return a", Keys.RETURN);
        wl.writeTo(By.id("console-input"), Keys.RETURN);

        wl.waitForElementToAppear(By.xpath("//li[contains(.,'Node[0]')]"));
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
