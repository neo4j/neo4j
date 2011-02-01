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

import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.RenderedWebElement;
import static org.junit.Assert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * Test that the webadmin data browser behaves as expected.
 */
public class DataBrowserWebTest extends WebDriverTest {
	
	@Test
	public void shouldBeAbleToCreateNewNode() throws InterruptedException {
		
		dataMenu.getElement().click();
		
		String originalNodeURI = nodeId.getAttribute("href");
		addNodeButton.getElement().click();
		
		nodeId.waitForAttributeToChangeFrom( "href", originalNodeURI );
	}
	
	@Test
	public void createRelationshipShouldHaveCurrentNodeFilledIn() {
		dataMenu.getElement().click();
		String expectedId = nodeId.getAttribute("href");

		addRelationshipButton.getElement().click();
		
		assertThat(addRelationshipDialogFromInput.getAttribute("value"), is(expectedId));
	}
	
	private ElementReference nodeId = new ElementReference(webDriver, By.className("mor_data_item_id")) {
		@Override
		public RenderedWebElement getElement() {
			return (RenderedWebElement) super.getElement().findElement(By.tagName("a"));
		}
	};
	
	private ElementReference addRelationshipDialogFromInput = new ElementReference(webDriver, By.id("mor_data_relationship_dialog_from"));
	
	private ElementReference addNodeButton = new ElementReference(webDriver, By.className("mor_data_add_node_button"));
	
	private ElementReference addRelationshipButton = new ElementReference(webDriver, By.className("mor_data_add_relationship"));
	
	
}
