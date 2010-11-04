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

package org.neo4j.server.webadmin.functional.web;

import java.util.ArrayList;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.RenderedWebElement;
import org.openqa.selenium.WebElement;

import static org.neo4j.server.webadmin.functional.web.IsVisible.isVisible;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Test that the webadmin HTTP console works and produces output as expected.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */

@Ignore
public class ConsoleFunctionalTest extends WebDriverTest {

	@Test
	public void shouldHaveConsoleWindow() {
		consoleMenu.getElement().click();
		assertThat(consoleWrap.getElement(), isVisible());
	}
	

	private List<String> resultOfTyping(CharSequence... keys) 
	{
		
		consoleInput.getElement().sendKeys(keys);
		waitForElementToBeVisible(consoleInput);

		List<String> result = new ArrayList<String>();
		for (WebElement el : consoleWrap.getElement().findElements(By.tagName("p"))) 
		{
			RenderedWebElement current = (RenderedWebElement) el;
			if (current.getAttribute("class") == "console-input") {
				result.clear();
			} else {
				result.add(current.getText());
			}
		}

		if (result.size() > 0) {
			result.remove(result.size() - 1);
		}

		return result;
	}
	
}
