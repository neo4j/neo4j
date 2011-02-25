/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.webadmin.webtest.IsVisible.isVisible;

<<<<<<< HEAD
=======
import java.util.List;

>>>>>>> 0e387346deb286261b486eab92b1067418d35ece
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.RenderedWebElement;
import org.openqa.selenium.WebElement;

/**
 * Test that the webadmin HTTP console works and produces output as expected.
 */
public class ConsoleWebTest extends WebDriverTest
{
<<<<<<< HEAD
=======
   
>>>>>>> 0e387346deb286261b486eab92b1067418d35ece
    @Test
    public void shouldHaveConsoleWindow()
    {
        consoleMenu.getElement().click();
        assertThat( consoleWrap.getElement(), isVisible() );
    } 
<<<<<<< HEAD
    
    @Test
    public void consoleShouldPrintStuffDirectedToSysError() {
=======

    @Test
    public void shouldOutputSysErrorWrites() throws InterruptedException {
>>>>>>> 0e387346deb286261b486eab92b1067418d35ece
    	consoleMenu.getElement().click();

        consoleInput.waitUntilVisible();
    	consoleInput.sendKeys("invalidoperation!¤", Keys.RETURN);
    	consoleInput.waitUntilVisible();
    	
<<<<<<< HEAD
    	consoleInput.sendKeys("$_g", Keys.RETURN);
=======
    	lastOutputLine.waitForTextToChangeFrom( "gremlin> invalidoperation!¤" );
    	assertThat(lastOutputLine.getText(), is( "==> 1 error" ));
>>>>>>> 0e387346deb286261b486eab92b1067418d35ece
    }
    
    private ElementReference consoleInput = new ElementReference(webDriver, By.id("mor_console_input"));

    private ElementReference lastOutputLine = new ElementReference(webDriver, By.id("mor_console")) {
        @Override
        public RenderedWebElement getElement() {
            List<WebElement> el = super.getElement().findElements(By.tagName("p"));
            return (RenderedWebElement) el.get( el.size() - 3 );
        }
    };
}
