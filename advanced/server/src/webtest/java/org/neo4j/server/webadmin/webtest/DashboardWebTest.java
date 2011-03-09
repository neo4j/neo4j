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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.webadmin.webtest.IsVisible.isVisible;

import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.openqa.selenium.By;


@RunWith( ThirdTimeIsTheCharmTestRunner.class )
public class DashboardWebTest extends WebDriverTest {

	@Test
	public void shouldHaveDashboardWindow() throws IOException {
		dashboardMenu.click();
		assertThat(dashboardValueTrackers.getElement(), isVisible());
	}
	
	@Test
	public void shouldBeAbleToSwitchBetweenCharts() {
		
		dashboardMenu.click();
		
		assertThat(primitivesChart.getElement(), isVisible());
		assertThat(memoryChart.getElement(), not(isVisible()));
		
		memoryChartTab.click();
		
		assertThat(memoryChart.getElement(), isVisible());
		assertThat(primitivesChart.getElement(), not(isVisible()));
		
		primitivesChartTab.click();
		
		assertThat(primitivesChart.getElement(), isVisible());
		assertThat(memoryChart.getElement(), not(isVisible()));
		
	}
	
	@Test
	public void shouldShowPrimitives() {
		dashboardMenu.click();
		
		GraphDbHelper dbHelper = new GraphDbHelper(server.getDatabase());
		
		assertThat(nodeCount.getText(), equalTo(String.valueOf(dbHelper.getNumberOfNodes())));
		assertThat(propertyCount.getText(), equalTo("0"));
		assertThat(relationshipCount.getText(), equalTo(String.valueOf(dbHelper.getNumberOfRelationships())));
		assertThat(relationshipTypeCount.getText(), equalTo("0"));
	}
	
	@Test
	public void nodeCountShouldUpdateWhenDatabaseIsModified() {
		dashboardMenu.click();
		GraphDbHelper dbHelper = new GraphDbHelper(server.getDatabase());
		
		int numberOfNodes = dbHelper.getNumberOfNodes();
		assertThat(nodeCount.getText(), equalTo(String.valueOf(numberOfNodes)));
		
		dbHelper.createNode();
		
		nodeCount.waitForTextToChangeFrom(String.valueOf(numberOfNodes));
		
		assertThat(nodeCount.getText(), equalTo(String.valueOf(numberOfNodes + 1)));
	}
	
	@Test
	public void relationshipCountShouldUpdateWhenDatabaseIsModified() {
		dashboardMenu.click();
		GraphDbHelper dbHelper = new GraphDbHelper(server.getDatabase());
		
		int numberOfRelationships = dbHelper.getNumberOfRelationships();
		assertThat(relationshipCount.getText(), equalTo(String.valueOf(numberOfRelationships)));
		
		dbHelper.createRelationship("SOME RELATIONSHIP");
		
		relationshipCount.waitForTextToChangeFrom(String.valueOf(numberOfRelationships));
		
		assertThat(relationshipCount.getText(), equalTo(String.valueOf(numberOfRelationships + 1)));
	}
	
	@Test
	public void relationshipTypeCountShouldUpdateWhenDatabaseIsModified() {
		dashboardMenu.click();
		GraphDbHelper dbHelper = new GraphDbHelper(server.getDatabase());
		
		assertThat(relationshipTypeCount.getText(), equalTo("0"));
		
		dbHelper.createRelationship("SOME RELATIONSHIP");
		
		relationshipTypeCount.waitForTextToChangeFrom("0");
		assertThat(relationshipTypeCount.getText(), equalTo("1"));
	}
	
	@Test
	public void shouldShowDiskUsage() {
		dashboardMenu.click();
		
		assertThat(totalUsage.getText().length(), greaterThan(0));
		assertThat(databaseSize.getText().length(), greaterThan(0));
		assertThat(logicalLogSize.getText().length(), greaterThan(0));
	}
	
	@Test
	public void shouldShowCacheInformation() {
		dashboardMenu.click();
		
		assertThat(Integer.parseInt( cachedNodes.getText() ), greaterThanOrEqualTo(0));
		assertThat(Integer.parseInt( cachedRelationships.getText() ), greaterThanOrEqualTo(0));
		assertThat(cacheType.getText().length(), greaterThan(0));
	}
	
	private ElementReference primitivesChartTab = new ElementReference(webDriver, By.id("mor_monitor_primitives_chart_tab"));
	private ElementReference primitivesChart = new ElementReference(webDriver, By.id("mor_monitor_primitives_chart"));
	private ElementReference memoryChartTab = new ElementReference(webDriver, By.id("mor_monitor_memory_chart_tab"));
	private ElementReference memoryChart = new ElementReference(webDriver, By.id("mor_monitor_memory_chart"));
	
	private ElementReference nodeCount = new ElementReference(webDriver, 
			By.xpath("//div[@id='mor_monitor_valuetrackers']//tr[contains(th,'Nodes')]/td"));
	
	private ElementReference propertyCount = new ElementReference(webDriver, 
			By.xpath("//div[@id='mor_monitor_valuetrackers']//tr[contains(th,'Properties')]/td"));
	
	private ElementReference relationshipCount = new ElementReference(webDriver, 
			By.xpath("//div[@id='mor_monitor_valuetrackers']//tr[contains(th,'Relationships')]/td"));
	
	private ElementReference relationshipTypeCount = new ElementReference(webDriver, 
			By.xpath("//div[@id='mor_monitor_valuetrackers']//tr[contains(th,'Relationship types')]/td"));
	
	private ElementReference totalUsage = new ElementReference(webDriver, 
			By.xpath("//div[@id='mor_monitor_valuetrackers']//tr[contains(th,'Total usage')]/td"));
	
	private ElementReference databaseSize = new ElementReference(webDriver, 
			By.xpath("//div[@id='mor_monitor_valuetrackers']//tr[contains(th,'Database size')]/td"));
	
	private ElementReference logicalLogSize = new ElementReference(webDriver, 
			By.xpath("//div[@id='mor_monitor_valuetrackers']//tr[contains(th,'Logical log size')]/td"));
	
	private ElementReference cachedNodes = new ElementReference(webDriver, 
			By.xpath("//div[@id='mor_monitor_valuetrackers']//tr[contains(th,'Cached nodes')]/td"));
	
	private ElementReference cachedRelationships = new ElementReference(webDriver, 
			By.xpath("//div[@id='mor_monitor_valuetrackers']//tr[contains(th,'Cached relationships')]/td"));
	
	private ElementReference cacheType = new ElementReference(webDriver, 
			By.xpath("//div[@id='mor_monitor_valuetrackers']//tr[contains(th,'Cache type')]/td"));
}
