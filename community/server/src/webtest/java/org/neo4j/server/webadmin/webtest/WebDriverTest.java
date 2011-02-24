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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.ServerBuilder;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.RenderedWebElement;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

public abstract class WebDriverTest {

    protected static WebDriver webDriver;
    protected NeoServerWithEmbeddedWebServer server;
    protected IntegrationTestHelper testHelper;
    protected GraphDbHelper dbHelper;

    private static final File targetHtmlDir = new File("target/classes/webadmin-html");
    private static final File srcHtmlDir = new File("src/main/resources/webadmin-html");
    
    private static final String webadminUri = "webadmin/index.html";
    
    @BeforeClass
    public static void copyHtmlToTargetDirectory() throws IOException {
        FileUtils.copyDirectory(srcHtmlDir, targetHtmlDir);
        webDriver = new FirefoxDriver();
    }
    
    @Before
    public void setupServer() throws IOException {
        server = ServerBuilder.server().withRandomDatabaseDir().withPassingStartupHealthcheck().build();
        server.start();
     
        testHelper = new IntegrationTestHelper(server);
        dbHelper = testHelper.getGraphDbHelper();
        
        webDriver.get(server.baseUri().toString() + webadminUri);

        dashboardMenu.waitUntilVisible();
    }

    @After
    public void stopServer() {
        testHelper = null;
        dbHelper = null;
        server.stop();
    }
    
    @AfterClass
    public static void afterClass() {
        webDriver.close();
    }
    
    protected void clickYesOnAllConfirmDialogs() {
        ((JavascriptExecutor)webDriver).executeScript("window.confirm = function(msg){return true;};");
    }
    
    protected void clickNoOnAllConfirmDialogs() {
        ((JavascriptExecutor)webDriver).executeScript("window.confirm = function(msg){return false;};");
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
    
    protected ElementReference errorList = new ElementReference(webDriver, By.id("mor_errors"));
    protected ElementReference lastError = new ElementReference(webDriver, By.id("mor_errors")) {
        @Override
        public RenderedWebElement getElement() {
            try {
                List<WebElement> el = super.getElement().findElements(By.tagName("li"));
                return (RenderedWebElement) el.get( el.size() - 1 );
            } catch(ArrayIndexOutOfBoundsException e) {
                throw new NoSuchElementException("There are no errors shown.");
            }
        }
    };
    
    protected ElementReference lastTooltip = new ElementReference(webDriver, By.className("tooltip-wrap"), true);
    
    protected ElementReference dialog = new ElementReference(webDriver, By.id("mor_dialog_content"));
}
