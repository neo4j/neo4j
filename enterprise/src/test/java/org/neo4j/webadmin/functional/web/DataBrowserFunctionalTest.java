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

package org.neo4j.webadmin.functional.web;

import org.junit.Ignore;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.RenderedWebElement;
import org.openqa.selenium.StaleElementReferenceException;

@Ignore
public class DataBrowserFunctionalTest extends WebDriverTest {

	
	@Test
	public void testCreateNewNode() throws InterruptedException {
		
		dataMenu.getElement().click();
		
		String originalNodeURI;
		try {
			originalNodeURI = nodeId.getElement().getAttribute("href");
		}catch(StaleElementReferenceException e) {
			originalNodeURI = nodeId.getElement().getAttribute("href");
		}
		addNodeButton.getElement().click();
		
		waitForAttributeToChangeFrom( nodeId, "href", originalNodeURI );
	}
	
	
	
	private ElementReference nodeId = new ElementReference() {
		public RenderedWebElement getElement() {
			return (RenderedWebElement) waitForElementToAppear(By.className("mor_data_item_id")).findElement(By.tagName("a"));
		}
	};
	
	
	private ElementReference addNodeButton = new ElementReference() {
		public RenderedWebElement getElement() {
			return waitForElementToAppear(By.className("mor_data_add_node_button"));
		}
	};
	
	
}
