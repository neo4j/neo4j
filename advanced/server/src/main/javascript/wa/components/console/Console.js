/*
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


wa.components.console.Console = (function($) {
    
    var me = {};
    
    me.basePage = $("<div></div>");
    me.ui = {};
    
    me.uiLoaded  = false;
    
    me.visible = false;
    
    me.consoleElement = null;
    
    me.history = [];
    me.currentHistoryIndex = -1;
    
    function getConsole() {
        return wa.Servers.getCurrentServer().manage.console;
    }
    
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
                    
                    if( me.uiLoaded === false ) {
                        me.uiLoaded = true;
                        me.basePage.setTemplateURL("templates/components/console/index.tp");
                        me.render();
                        me.consoleInit();
                    }
                    
                    me.focusOnInputElement();
                    
                } else {
                    me.visible = false;
                }
            },
            
            /**
             * Send a console command up to the server to be evaluated.
             * 
             * @param statement
             *            is the statement string
             * @param cb
             *            (optional) callback that is called with the result
             *            object. If this is not specified, the result will be
             *            printed to the console.
             */
            evaluate : function(statement, cb, showStatement) {
                var cb = cb || me.evalCallback,
                    showStatement = showStatement === false ? false : true;
                
                if(showStatement){
                    me.writeConsoleLine(statement, null, "console-input");
                    
                    if( statement.length > 0) {
                        me.api.pushHistory(statement);
                    }
                }
                
                me.hideInput();
                
                getConsole().exec(statement, "awesome", (function(statement, cb) {
                    return function(data) {
                        var lines;
                        if(_.isString(data) && data.length > 0) {
                            lines =  me.stripTrailingNewline(data).split("\n");
                        } else {
                            lines = [];
                        }
                        cb(statement, lines);
                        
                        me.showInput();
                    };
                })(statement, cb));
                
            },
            
            serverChanged : function(server) {
                me.consoleInit();
            },
            
            pushHistory : function(cmd) {
                me.history.push(cmd);
                me.currentHistoryIndex = me.history.length - 1;
            },
            
            prevHistory : function() {
                if( me.currentHistoryIndex >= 0 && me.history.length > me.currentHistoryIndex ) {
                    me.currentHistoryIndex--;
                    return me.history[me.currentHistoryIndex + 1];
                } else if (me.history.length > 0) {
                    return me.history[0];
                } else {
                    return "";
                }
            },
            
            nextHistory : function() {
                if( me.history.length > (me.currentHistoryIndex + 1) ) {
                    me.currentHistoryIndex++;
                    return me.history[me.currentHistoryIndex];
                } else {
                    return "";
                }
            }
            
    };
    
    // 
    // PRIVATE
    //
    
    me.render = function() {
        
        me.basePage.processTemplate({
            server : me.server
        });
        
        me.consoleWrap      = $(".mor_console_wrap");
        me.consoleElement   = $("#mor_console");
        me.consoleInputWrap = $("#mor_console_input_wrap");
        me.consoleInput     = $("#mor_console_input");
        
    };
    
    /**
     * Clear console and send init command to server.
     * Checks that the console is visible and a server
     * is available.
     */
    me.consoleInit = function() {
        if(me.visible && wa.Servers.getCurrentServer()) {
            
            if( me.server != wa.Servers.getCurrentServer()) {
                me.server = wa.Servers.getCurrentServer();
                me.clear();
                me.api.evaluate("init()", null, false);
            }
        }
    };
    
    me.clear = function() {
        $("#mor_console .console-input").remove();
        $("#mor_console_input").val("");
    };
    
    me.hideInput = function() {
    	$("#mor_console_input_wrap").hide();
    };
    
    me.showInput = function() {
    	$("#mor_console_input_wrap").show();
    	me.scrollToBottomOfConsole();
    };
    
    me.focusOnInputElement = function() {
        me.showInput();
    	$("#mor_console_input").focus();
    };
    
    /**
     * Default callback for evaluated console statements. Prints the result to
     * the ui console.
     */
    me.evalCallback = function(originalStatement, lines) {
        _.each(lines, function(line) {
            me.writeConsoleLine(line, '==> ');
        });
    };
    
    me.writeConsoleLine = function(line, prepend, clazz) {
        var prepend = prepend || "gremlin> ";
        var clazz = clazz || "";
        var line = _.isString(line) ? line : "";
        me.consoleInputWrap.before($("<p> " + wa.htmlEscape(prepend + line) + "</p>").addClass(clazz));
        me.scrollToBottomOfConsole();
    };
    
    me.scrollToBottomOfConsole = function() {
        me.consoleWrap[0].scrollTop = me.consoleWrap[0].scrollHeight;
    };
    
    me.stripTrailingNewline = function(str) {
        if(str.substr(-1) === "\n") {
            return str.substr(0, str.length - 1);
        } else {
            return str;
        }
    };
    
    //
    // CONSTRUCT
    //
    
    /**
     * Look for enter-key press on input field.
     */
    $("#mor_console_input").live("keyup", function(ev) {
        if( ev.keyCode === 13 ) { // ENTER
            me.api.evaluate(me.consoleInput.val());
            me.consoleInput.val("");
        } else if (ev.keyCode === 38) { // UP
            me.consoleInput.val(me.api.prevHistory());
        } else if (ev.keyCode === 40) { // DOWN
            me.consoleInput.val(me.api.nextHistory());
        }
    });
    
    $("#mor_console").live("click", function(ev) {
    	if(ev.target.id === "mor_console") {
    		me.focusOnInputElement();
    	}
    });
    
    return me.api;
    
})(jQuery);

//
// REGISTER STUFF
//

wa.ui.Pages.add("console",wa.components.console.Console);
wa.ui.MainMenu.add({ label : "Console", pageKey:"console", index:2, requiredServices:['console'], perspectives:['server']});

wa.bind("ui.page.changed", wa.components.console.Console.pageChanged);
wa.bind("servers.current.changed",  wa.components.console.Console.serverChanged);