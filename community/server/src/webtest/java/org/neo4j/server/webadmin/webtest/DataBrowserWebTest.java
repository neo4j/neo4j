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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.webadmin.webtest.IsVisible.isVisible;

import java.util.List;

import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.RenderedWebElement;
import org.openqa.selenium.WebElement;

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
	
	@Test
    public void createRelationshipShouldListAvailableRelationshipTypes() {

	    String testType = "BANANA";
	    String irrellevant = "--thiswillneverbeatype--";
	    String [] expectedItems = new String[] {irrellevant, irrellevant, testType};
	    
 	    testHelper.getGraphDbHelper().createRelationship( testType );
	    
	    dataMenu.getElement().click();
        addRelationshipButton.getElement().click();
        
        List<WebElement> opts = addRelationshipDialogTypeDropdown.findElements( By.tagName( "option" ) );
        
        assertThat(opts.size(), is( expectedItems.length ));
        
        for(int i=0; i<expectedItems.length; i++) {
            if( ! expectedItems[i].equals(irrellevant)) {
                assertThat(opts.get(i).getValue(), is(expectedItems[i]));
            }
        }
    }
	
	@Test
    public void shouldNotBeAbleToCreateRelationshipWithoutType() {
        
	    String someNodeId = testHelper.nodeUri(dbHelper.createNode());
	    
	    int initialRelCount = dbHelper.getNumberOfRelationships();
	    
        dataMenu.getElement().click();
        addRelationshipButton.getElement().click();
        
        addRelationshipDialogToInput.getElement().sendKeys( someNodeId );
        
        // Remove the initial type that is always entered 
        int backs = 20;
        while(backs-- >= 0) { addRelationshipDialogTypeInput.getElement().sendKeys( Keys.BACK_SPACE ); }
        
        addRelationshipDialogSave.click();
        
        errorList.waitUntilVisible();
        
        assertThat(dialog.getElement(), isVisible());
        assertThat(dbHelper.getNumberOfRelationships(), is(initialRelCount));
        
    }
	
	@Test
    public void hittingReturnShouldFinishCreateRelationshipDialog() {
        
        String someNodeId = testHelper.nodeUri(dbHelper.createNode());
        
        int initialRelCount = dbHelper.getNumberOfRelationships();
        
        dataMenu.getElement().click();
        addRelationshipButton.getElement().click();
        
        addRelationshipDialogToInput.getElement().sendKeys( someNodeId, Keys.RETURN );
        
        dialog.waitUntilNotVisible();
        
        assertThat(dbHelper.getNumberOfRelationships(), is(initialRelCount + 1));
        
    }
	
	private ElementReference nodeId = new ElementReference(webDriver, By.className("mor_data_item_id")) {
		@Override
		public RenderedWebElement getElement() {
			return (RenderedWebElement) super.getElement().findElement(By.tagName("a"));
		}
	};
	
	
	private ElementReference addRelationshipDialogFromInput = new ElementReference(webDriver, By.id("mor_data_relationship_dialog_from"));
	private ElementReference addRelationshipDialogToInput = new ElementReference(webDriver, By.id("mor_data_relationship_dialog_to"));
	private ElementReference addRelationshipDialogTypeDropdown = new ElementReference(webDriver, By.id("mor_data_relationship_available_types"));
	private ElementReference addRelationshipDialogTypeInput = new ElementReference(webDriver, By.id("mor_data_relationship_dialog_type"));
	private ElementReference addRelationshipDialogSave = new ElementReference(webDriver, By.className( "mor_data_relationship_dialog_save"));
	private ElementReference addNodeButton = new ElementReference(webDriver, By.className("mor_data_add_node_button"));
	private ElementReference addRelationshipButton = new ElementReference(webDriver, By.className("mor_data_add_relationship"));
	
	private ElementReference lastRelationshipInList = new ElementReference(webDriver, By.xpath( "//table[@class='mor_fancy data-table']/tbody/tr[last()]/td[2]/a/@href" ));
	
	
}
