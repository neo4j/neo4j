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

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.webadmin.webtest.IsVisible.isVisible;

import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;
import org.openqa.selenium.By;

@Ignore
public class JmxWebTest extends WebDriverTest {
 
	@Test
	public void shouldHaveJmxWindow() throws IOException {
		jmxMenu.click();
		assertThat(jmxList.getElement(), isVisible());
	}
	
	@Test
	public void neo4jBeanShouldBeOnTop() {
		jmxMenu.click();
		
		assertThat(jmxList.findElement(By.tagName("h2")).getText(), is("org.neo4j"));
	}
	
	@Test
	public void shouldBeAbleToReadKernelBean() {
		jmxMenu.click();
		
		kernelBeanButton.click();
		
		assertThat(storeIdContainer.getText().length(), greaterThan(0));
	}
	
	
	private ElementReference jmxList = new ElementReference(webDriver, By.className("mor_jmx_list"));
	private ElementReference kernelBeanButton = new ElementReference(webDriver, By.xpath("//div[@id='mor_pages']//li[contains(a, 'Kernel')]/a"));
	
	private ElementReference storeIdContainer = new ElementReference(webDriver, By.xpath("//table[@class='mor_jmx_table']//tr[contains(td[@class='mor_jmx_table_name']/h3, 'StoreId')]/td[2]"));
}
