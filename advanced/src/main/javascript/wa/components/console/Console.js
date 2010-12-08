/*
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


wa.components.console.Console = (function($) {
    
    var me = {};
    
    me.basePage = $("<div></div>");
    me.ui = {};
    
    me.uiLoaded  = false;
    me.visible = false;
    
    me.consoleElement = null;
    
    me.getConsoleService = function() {
        return wa.Servers.getCurrentServer().manage.console;
    };
    
    //
    // PUBLIC
    //
    
    me.api = {
            
            getPage :  function() {
                return me.basePage;
            },
            
            pageChanged : function(ev) {
                
                if(ev.data === "console") {
                    
                    me.visible = true;

                    if(me.terminal) {
                        me.terminal.open();
                        me.terminal.restoreScreen();
                    }

                    if( me.uiLoaded === false ) {
                        me.uiLoaded = true;
                        me.basePage.setTemplateURL("templates/components/console/index.tp");
                        me.render();
                    }

                } else {
                    me.visible = false;
                    if(me.terminal)
                    {
                        me.terminal.backupScreen();
                        me.terminal.close();
                    }
                }
            }
            
    };
    
    // 
    // PRIVATE
    //

    me.termHandler = function(){
        this.newLine();

        var term = this;

        if (term.lineBuffer === "invaders" ) {
            TermlibInvaders.start(term);
        } else {
            me.getConsoleService().exec(term.lineBuffer, function(lines) {
            	
                for(var i=0, l=lines.length; i < l; i++ ) {
                    term.write(lines[i]);
                    term.newLine();
                }

                term.prompt();
            });
        }
    };
    
    me.render = function() {
        
        me.basePage.processTemplate({
            server : me.server
        });
        
        me.consoleWrap = $(".mor_console_wrap");

        me.terminal = new Terminal({
            "termDiv": 'mor_console',
            "handler": me.termHandler,
            "wrapping": true
        });
        me.terminal.open();
        
    };
    
    return me.api;
    
})(jQuery);

//
// REGISTER STUFF
//

wa.ui.Pages.add("console",wa.components.console.Console);
wa.ui.MainMenu.add({ label : "Console", pageKey:"console", index:2, requiredServices:['console'], perspectives:['server']});

wa.bind("ui.page.changed", wa.components.console.Console.pageChanged);