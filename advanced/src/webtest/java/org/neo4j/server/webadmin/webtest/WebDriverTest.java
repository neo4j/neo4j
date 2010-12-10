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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerBuilder;
import org.openqa.selenium.By;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.RenderedWebElement;
import org.openqa.selenium.StaleElementReferenceException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.firefox.FirefoxDriver;

public abstract class WebDriverTest {

    protected WebDriver webDriver = new FirefoxDriver();
    protected NeoServer server;

    private static final File targetHtmlDir = new File("target/classes/webadmin-html");
    private static final File srcHtmlDir = new File("src/main/resources/webadmin-html");
    
    @BeforeClass
    public static void copyHtmlToTargetDirectory() throws IOException {
        FileUtils.copyDirectory(srcHtmlDir, targetHtmlDir);
    }
    
    @Before
    public void setupServer() throws IOException {
        server = ServerBuilder.server().withRandomDatabaseDir().withPassingStartupHealthcheck().build();
        server.start();
        
        String url = server.webadminUri().toString() + "index-no-feedback.html";
        System.out.println("testing " + url);
        webDriver.get(url);

        dashboardMenu.waitUntilVisible();
    }

    @After
    public void stopServer() {
        webDriver.close();
        server.stop();
    }

    protected ElementReference dashboardMenu = new ElementReference(webDriver, By.id("mainmenu-dashboard")) {
        @Override
        public RenderedWebElement getElement() {
            return (RenderedWebElement) super.getElement().findElement(By.tagName("a"));
        }
    };

    protected ElementReference consoleMenu = new ElementReference(webDriver, By.id("mainmenu-console")) {
        @Override
        public RenderedWebElement getElement() {
            return (RenderedWebElement) super.getElement().findElement(By.tagName("a"));
        }
    };

    protected ElementReference configMenu = new ElementReference(webDriver, By.id("mainmenu-config")) {
        @Override
    	public RenderedWebElement getElement() {
            return (RenderedWebElement) super.getElement().findElement(By.tagName("a"));
        }
    };

    protected ElementReference dataMenu = new ElementReference(webDriver, By.id("mainmenu-data")) {
        @Override
    	public RenderedWebElement getElement() {
            return (RenderedWebElement) super.getElement().findElement(By.tagName("a"));
        }
    };

    protected ElementReference backupMenu = new ElementReference(webDriver, By.id("mainmenu-backup")) {
        @Override
    	public RenderedWebElement getElement() {
            return (RenderedWebElement) super.getElement().findElement(By.tagName("a"));
        }
    };

    protected ElementReference ioMenu = new ElementReference(webDriver,By.id("mainmenu-io") ) {
        @Override
    	public RenderedWebElement getElement() {
            return (RenderedWebElement) super.getElement().findElement(By.tagName("a"));
        }
    };

    protected ElementReference jmxMenu = new ElementReference(webDriver,By.id("mainmenu-jmx")) {
        @Override
    	public RenderedWebElement getElement() {
            return (RenderedWebElement) super.getElement().findElement(By.tagName("a"));
        }
    };

    protected ElementReference configDatabaseLocation = new ElementReference(webDriver, By.id("mor_setting_db.root"));

    protected ElementReference consoleWrap = new ElementReference(webDriver, By.className("mor_console_wrap"));

    protected ElementReference consoleInput = new ElementReference(webDriver, By.id("mor_console_input"));

    protected ElementReference dashboardValueTrackers = new ElementReference(webDriver, By.id("mor_monitor_valuetrackers"));

}
