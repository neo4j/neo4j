/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.webadmin;

import static org.openqa.selenium.OutputType.FILE;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.helpers.ServerBuilder;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.webdriver.WebDriverFacade;
import org.neo4j.server.webdriver.WebadminWebdriverLibrary;
import org.neo4j.test.JavaTestDocsGenerator;
import org.neo4j.test.TestData;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebDriver;

public abstract class AbstractWebadminTest {
    
    public @Rule
    TestData<JavaTestDocsGenerator> gen = TestData.producedThrough( JavaTestDocsGenerator.PRODUCER );

    protected static WebadminWebdriverLibrary wl;
    
    private static NeoServerWithEmbeddedWebServer server;
    private static WebDriverFacade webdriverFacade;

    @BeforeClass
    public static void setup() throws Exception {
        server = ServerBuilder.server().build();
        server.start();
        
        webdriverFacade = new WebDriverFacade();
        wl = new WebadminWebdriverLibrary(webdriverFacade,server.baseUri().toString());
    }
    
    @After
    public void doc() {
        gen.get().document("target/docs","webadmin");
    }

    protected void captureScreenshot( String string )
    {
        WebDriver webDriver = wl.getWebDriver();
        if(webDriver instanceof TakesScreenshot) 
        {
            try
            {
                File screenshotFile = ((TakesScreenshot)webDriver).getScreenshotAs(FILE);
                System.out.println(screenshotFile.getAbsolutePath());
                File dir = new File("target/docs/webadmin/images");
                dir.mkdirs();
                String imageName = string+".png";
                copyFile( screenshotFile, new File(dir, imageName) );
                gen.get().addImageSnippet(string, imageName, gen.get().getTitle());
                
            }
            catch ( Exception e )
            {
                e.printStackTrace();
            }
        }    
    }
    
    @Before
    public void cleanDatabase() {
        ServerHelper.cleanTheDatabase(server);
    }
    
    @AfterClass
    public static void tearDown() throws Exception {
        webdriverFacade.closeBrowser();
        server.stop();
    }
    
    private static void copyFile(File sourceFile, File destFile) throws IOException {
        if(!destFile.exists()) {
            destFile.createNewFile();
        }

        FileChannel source = null;
        FileChannel destination = null;

        try {
            source = new FileInputStream(sourceFile).getChannel();
            destination = new FileOutputStream(destFile).getChannel();
            destination.transferFrom(source, 0, source.size());
        }
        finally {
            if(source != null) {
                source.close();
            }
            if(destination != null) {
                destination.close();
            }
        }
    }
}
