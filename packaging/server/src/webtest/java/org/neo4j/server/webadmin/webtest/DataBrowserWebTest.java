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
import org.neo4j.graphdb.Node;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.RenderedWebElement;
import org.openqa.selenium.WebElement;

/**
 * Test that the webadmin data browser behaves as expected.
 */
public class DataBrowserWebTest extends WebDriverTest {
	
	@Test
	public void shouldBeAbleToCreateNewNode() throws InterruptedException {
		
		dataMenu.click();
		
		String originalNodeURI = nodeId.getAttribute("href");
		addNodeButton.click();
		
		nodeId.waitForAttributeToChangeFrom( "href", originalNodeURI );
	}
	
	@Test
    public void shouldBeAbleToCreateRelationship() {
        
        String someNodeId = testHelper.nodeUri(dbHelper.createNode());
        
        int initialRelCount = dbHelper.getNumberOfRelationships();
        
        dataMenu.click();
        addRelationshipButton.click();
        
        addRelationshipDialogToInput.sendKeys( someNodeId );
        saveNewRelationshipButton.click();
        
        dialog.waitUntilNotVisible();
        
        assertThat(dbHelper.getNumberOfRelationships(), is(initialRelCount + 1));
        
    }
    
    @Test
    public void shouldBeAbleToSaveStringProperty() {

        String propertyKey = "mykey";
        
        dataMenu.click();
        addPropertyButton.click();
        firstPropertyKeyInput.sendKeys( propertyKey, Keys.RETURN );
        firstPropertyValueInput.sendKeys( "\"myvalue\"", Keys.RETURN );
        
        savePropertiesButton.waitForTextToChangeTo( "Saved" );
        
        Node n = testHelper.getDatabase().getReferenceNode();
        assertThat( (String)n.getProperty( propertyKey ), is( "myvalue" ));
        
    }
    
    @Test
    public void shouldBeAbleToSaveIntegerProperty() {

        String propertyKey = "mykey";
        
        dataMenu.click();
        addPropertyButton.click();
        firstPropertyKeyInput.sendKeys( propertyKey, Keys.RETURN );
        firstPropertyValueInput.sendKeys( "666", Keys.RETURN );
        
        savePropertiesButton.waitForTextToChangeTo( "Saved" );
        
        Node n = testHelper.getDatabase().getReferenceNode();
        assertThat( (Integer)n.getProperty( propertyKey ), is( 666));
        
    }
    
    @Test
    public void shouldBeAbleToSaveFloatProperty() {

        String propertyKey = "mykey";
        
        dataMenu.click();
        addPropertyButton.click();
        firstPropertyKeyInput.sendKeys( propertyKey, Keys.RETURN );
        firstPropertyValueInput.sendKeys( "66.6", Keys.RETURN );
        
        savePropertiesButton.waitForTextToChangeTo( "Saved" );
        
        Node n = testHelper.getDatabase().getReferenceNode();
        assertThat( (Double)n.getProperty( propertyKey ), is( 66.6));
        
    }
    
    @Test
    public void shouldBeAbleToSaveArrayOfStringsProperty() {

        String propertyKey = "mykey";
        
        dataMenu.click();
        addPropertyButton.click();
        firstPropertyKeyInput.sendKeys( propertyKey, Keys.RETURN );
        firstPropertyValueInput.sendKeys( "[\"one\",\"two\"]", Keys.RETURN );
        
        savePropertiesButton.waitForTextToChangeTo( "Saved" );
        
        Node n = testHelper.getDatabase().getReferenceNode();
        String[] value = (String[]) n.getProperty( propertyKey );
        assertThat(value.length,is(2));
        assertThat(value[0], is("one"));
        assertThat(value[1], is("two"));
    }

    @Test
    public void shouldBeAbleToSaveArrayOfIntegersProperty() {

        String propertyKey = "mykey";
        
        dataMenu.click();
        addPropertyButton.click();
        firstPropertyKeyInput.sendKeys( propertyKey, Keys.RETURN );
        firstPropertyValueInput.sendKeys( "[1,2]", Keys.RETURN );
        
        savePropertiesButton.waitForTextToChangeTo( "Saved" );
        
        Node n = testHelper.getDatabase().getReferenceNode();
        Integer[] value = (Integer[]) n.getProperty( propertyKey );
        assertThat(value.length,is(2));
        assertThat(value[0], is(1));
        assertThat(value[1], is(2));
    }
    
    @Test
    public void shouldBeAbleToSaveArrayOfDoublesProperty() {

        String propertyKey = "mykey";
        
        dataMenu.click();
        addPropertyButton.click();
        firstPropertyKeyInput.sendKeys( propertyKey, Keys.RETURN );
        firstPropertyValueInput.sendKeys( "[1.1,2.2]", Keys.RETURN );
        
        savePropertiesButton.waitForTextToChangeTo( "Saved" );
        
        Node n = testHelper.getDatabase().getReferenceNode();
        Double[] value = (Double[]) n.getProperty( propertyKey );
        assertThat(value.length,is(2));
        assertThat(value[0], is(1.1));
        assertThat(value[1], is(2.2));
    }
    
    @Test
    public void shouldNotBeAbleToSaveJSONMapsAsProperty() {

        String propertyKey = "mykey";
        
        dataMenu.click();
        addPropertyButton.click();
        firstPropertyKeyInput.sendKeys( propertyKey, Keys.RETURN );
        firstPropertyValueInput.sendKeys( "{\"a\":1}", Keys.RETURN );
       
        lastTooltip.waitForTextToChangeTo( "Maps are not supported property values." );
    }
    

    
    @Test
    public void alphanumericPropertyValueShouldBeConvertedToString() {

        String propertyKey = "mykey";
        
        dataMenu.click();
        addPropertyButton.click();
        firstPropertyKeyInput.sendKeys( propertyKey, Keys.RETURN );
        firstPropertyValueInput.sendKeys( "alpha123_-", Keys.RETURN );
        
        assertThat(firstPropertyValueInput.getValue(), is("\"alpha123_-\""));
       
        lastTooltip.waitForTextToChangeTo( "Your input has been automatically converted to a string." );
        
        savePropertiesButton.waitForTextToChangeTo( "Saved" );
        
        Node n = testHelper.getDatabase().getReferenceNode();
        assertThat( (String)n.getProperty( propertyKey ), is("alpha123_-"));
    }
    
    @Test
    public void shouldNotBeAbleToSaveInvalidJSONProperty() {

        String propertyKey = "mykey";
        
        dataMenu.click();
        addPropertyButton.click();
        firstPropertyKeyInput.sendKeys( propertyKey, Keys.RETURN );
        firstPropertyValueInput.sendKeys( "\"a\":!!1}", Keys.RETURN );
       
        lastTooltip.waitForTextToChangeTo( "This does not appear to be a valid JSON value." );
    }
	
	@Test
    public void shouldBeAbleToRemoveNode() {
        
	    long nodeId = dbHelper.createNode();
        String nodeUri = testHelper.nodeUri(nodeId);
        dataMenu.click();
        
        int nodeCount = dbHelper.getNumberOfNodes();
        
        getByUrlInput.sendKeys( nodeUri, Keys.RETURN );
        
        currentNodeId.waitForAttributeToBe( "href", nodeUri );
        
        clickYesOnAllConfirmDialogs();
        
        deleteNodeButton.click();
        
        // Should redirect to reference node
        currentNodeId.waitForAttributeToBe( "href", testHelper.nodeUri(0l) );
        
        assertThat(dbHelper.getNumberOfNodes(), is(nodeCount - 1));
        
    }
	
	@Test
    public void shouldBeAbleToRemoveReferenceNodeAndThenGoToSomeOtherNode() {
        
        String nodeUri = testHelper.nodeUri(dbHelper.createNode());
        String referenceNodeUri = testHelper.nodeUri(0l);
        
        dataMenu.click();
        
        int nodeCount = dbHelper.getNumberOfNodes();
        currentNodeId.waitForAttributeToBe( "href", referenceNodeUri );
        
        clickYesOnAllConfirmDialogs();
        
        // Delete root node
        deleteNodeButton.click();
        
        // We should see "no node found blah blah"
        notFoundPanel.waitUntilVisible();

        getByUrlInput.sendKeys( nodeUri, Keys.RETURN );
        currentNodeId.waitForAttributeToBe( "href", nodeUri );
        
        assertThat(dbHelper.getNumberOfNodes(), is(nodeCount - 1));
        
    }
	
	@Test
	public void createRelationshipShouldHaveCurrentNodeFilledIn() {
		dataMenu.click();
		String expectedId = nodeId.getAttribute("href");

		addRelationshipButton.click();
		
		assertThat(addRelationshipDialogFromInput.getAttribute("value"), is(expectedId));
	}
	
	@Test
    public void createRelationshipShouldListAvailableRelationshipTypes() {

	    String testType = "BANANA";
	    String irrellevant = "--thiswillneverbeatype--";
	    String [] expectedItems = new String[] {irrellevant, irrellevant, testType};
	    
 	    testHelper.getGraphDbHelper().createRelationship( testType );
	    
	    dataMenu.click();
        addRelationshipButton.click();
        
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
	    
        dataMenu.click();
        addRelationshipButton.click();
        
        addRelationshipDialogToInput.sendKeys( someNodeId );
        
        // Remove the initial type that is always entered 
        int backs = 20;
        while(backs-- >= 0) { addRelationshipDialogTypeInput.getElement().sendKeys( Keys.BACK_SPACE ); }
        
        addRelationshipDialogSave.click();
        
        errorList.waitUntilVisible();
        
        assertThat(dialog.getElement(), isVisible());
        assertThat(dbHelper.getNumberOfRelationships(), is(initialRelCount));
        
    }
	
	@Test
    public void creatingRelationshipToInvalidNodeShouldShowError() {
        
        dataMenu.click();
        addRelationshipButton.click();
        
        addRelationshipDialogToInput.sendKeys( "jibberish" );
        
        addRelationshipDialogSave.click();
        lastError.waitUntilVisible();
        
        assertThat(dialog.getElement(), isVisible());
        
    }
	
	@Test
    public void hittingReturnShouldFinishCreateRelationshipDialog() {
        
        String someNodeId = testHelper.nodeUri(dbHelper.createNode());
        
        int initialRelCount = dbHelper.getNumberOfRelationships();
        
        dataMenu.click();
        addRelationshipButton.click();
        
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
	private ElementReference deleteNodeButton = new ElementReference(webDriver, By.className("mor_data_delete_node_button"));
	private ElementReference addRelationshipButton = new ElementReference(webDriver, By.className("mor_data_add_relationship"));
	private ElementReference saveNewRelationshipButton = new ElementReference(webDriver, By.className("mor_data_relationship_dialog_save"));
	
	private ElementReference addPropertyButton = new ElementReference(webDriver, By.className("mor_data_add_property"));
	private ElementReference savePropertiesButton = new ElementReference(webDriver, By.className("mor_data_save"));
	private ElementReference firstPropertyKeyInput = new ElementReference(webDriver, By.className("mor_data_key_input"));
	private ElementReference firstPropertyValueInput = new ElementReference(webDriver, By.className("mor_data_value_input"));
	
	private ElementReference lastPropertyKeyInput = new ElementReference(webDriver, By.className("mor_data_key_input")){
	    @Override
	    public RenderedWebElement getElement() {
	        List<WebElement> elements = webDriver.findElements( selector );
	        if(elements.size() > 0) {
	            return (RenderedWebElement) elements.get( elements.size() - 1 );
	        } else {
	            throw new NoSuchElementException("Cannot find last property key");
	        }
	    }
	};
	private ElementReference lastPropertyValueInput = new ElementReference(webDriver, By.className("mor_data_value_input")){
        @Override
        public RenderedWebElement getElement() {
            List<WebElement> elements = webDriver.findElements( selector );
            if(elements.size() > 0) {
                return (RenderedWebElement) elements.get( elements.size() - 1 );
            } else {
                throw new NoSuchElementException("Cannot find last property key");
            }
        }
    };
	
	private ElementReference getByUrlInput = new ElementReference(webDriver, By.id("mor_data_get_id_input"));
	
	
	private ElementReference notFoundPanel = new ElementReference(webDriver, By.className( "mor_data_item_notfound" ));
	
	private ElementReference currentNodeId = new ElementReference(webDriver, By.className( "mor_data_current_node_id" ));
	
	
}
