/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.webadmin.webtest;

import static org.junit.Assert.assertThat;
import static org.neo4j.server.webadmin.webtest.IsVisible.isVisible;

import org.junit.Ignore;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;

/**
 * Test that the webadmin HTTP console works and produces output as expected.
 */
public class ConsoleWebTest extends WebDriverTest
{
   
	
    @Test
    public void shouldHaveConsoleWindow()
    {
        consoleMenu.getElement().click();
        assertThat( consoleWrap.getElement(), isVisible() );
    } 
    
    // Selenium borks charcodes somewhere from here to javascript-land in the browser,
    // and so sending char codes like "enter" ends up being something completely different
    // by the time javascript catches it. Until that is solved, we can't write tests for the console.
    @Ignore
    @Test
    public void consoleShouldWork() {
    	consoleMenu.getElement().click();
    	
    	consoleInput.sendKeys("$_g", Keys.ENTER);
    }
    
    private ElementReference consoleInput = new ElementReference(webDriver, By.id("mor_console_input"));
}
