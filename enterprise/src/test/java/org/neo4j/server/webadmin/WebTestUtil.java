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

package org.neo4j.webadmin;

import org.openqa.selenium.By;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.RenderedWebElement;
import org.openqa.selenium.WebDriver;

import static org.junit.Assert.assertTrue;

/**
 * Helper methods used by the web tests.
 *
 */
public class WebTestUtil {
	
	public static void assertElementAppears(WebDriver driver, By findBy) {
		assertElementAppears("Element should appear", driver, findBy);
	}
	
	public static void assertElementAppears(String message, WebDriver driver, By findBy) {
		
		boolean elementAppeared = false;
		long end = System.currentTimeMillis() + 10000;
        while (System.currentTimeMillis() < end) {
            // Browsers which render content (such as Firefox and IE)
            // return "RenderedWebElements"
        	RenderedWebElement resultsDiv = null;
        	try{
                resultsDiv = (RenderedWebElement) driver.findElement(findBy);
                assertTrue(resultsDiv.getValueOfCssProperty("display") != "none");
        	}
        	catch(NoSuchElementException ex)
        	{
        		;
        	}
            // If results have been returned,
            // the results are displayed in a drop down.
            if (resultsDiv != null) {
            	elementAppeared = true;
            	break;
            }
        }
		
		assertTrue(message, elementAppeared);
	}
	
}
