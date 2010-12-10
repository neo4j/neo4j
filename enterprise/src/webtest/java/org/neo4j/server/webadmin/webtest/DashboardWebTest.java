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

import java.io.IOException;
import org.openqa.selenium.By;

import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.neo4j.server.webadmin.webtest.IsVisible.isVisible;
import static org.hamcrest.core.IsNot.not;

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
		
	}
	
	private ElementReference primitivesChartTab = new ElementReference(webDriver, By.id("mor_monitor_primitives_chart_tab"));
	private ElementReference primitivesChart = new ElementReference(webDriver, By.id("mor_monitor_primitives_chart"));
	private ElementReference memoryChartTab = new ElementReference(webDriver, By.id("mor_monitor_memory_chart_tab"));
	private ElementReference memoryChart = new ElementReference(webDriver, By.id("mor_monitor_memory_chart"));
	
}
