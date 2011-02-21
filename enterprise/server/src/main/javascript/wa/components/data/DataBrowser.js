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

/**
 * Data browser.
 */
wa.components.data.DataBrowser = (function($) {
    
    var me = {};
    
    me.basePage = $("<div></div>");
    me.ui = {};

	me.propertiesToListManager = wa.components.data.PropertiesToListManager;
    me.uiLoaded  = false;
    me.server = null;
    
    me.visible = false;
    
    me.dataUrl = null;
    me.currentItem = null;
    me.nodeCache = {};
    
    /**
     * Pagination counter for the related nodes table.
     */
    me.currentRelatedNodePage = 0;
    
    /**
     * Number of related nodes to show per page.
     */
    me.relatedNodesPerPage = 10;

    /**
     * A promise for a reloading round to complete.
     * Used to stack reloading calls.
     */
    me.reloadPromise = neo4j.Promise.fulfilled();
    
    /**
     * A promise for a rendering round to complete.
     * Used to stack rendering calls.
     */
    me.processTemplatePromise = neo4j.Promise.fulfilled();
    
    //
    // PUBLIC
    //
    
    me.api = {
            
            getPage :  function() {
                return me.basePage;
            },
            
            pageChanged : function(ev) {
                
                if(ev.data === "data") {
                    
            		me.visible = true;
                
                    if( me.uiLoaded === false ) {
                    	me.uiLoaded = true;
                        me.basePage.setTemplateURL("templates/components/data/index.tp");
	                    
	                    me.reload();
                    }
                	
                } else {
                    me.visible = false;
                }
            },
            
            serverChanged : function(ev) {
                
                me.server = ev.data.server;
                
                // If the monitor page is currently visible
                if( me.visible === true ) {
                	me.reload();
                }
            },
            
            init : function() {
                wa.bind('data.listnames.changed',me.listNamesChanged);
                $( window ).bind( "hashchange", me.hashchange );
                me.hashchange();
            },
            
            setDataUrl : function(url) {
            	$.bbq.pushState({ dataurl: url });
            },
            
            /**
             * Get the current server being browsed.
             */
            getServer : function() {
            	return wa.Servers.getCurrentServer();
            },
            
            /**
             * Return the current item being viewed.
             */
            getItem : function() {
            	return me.currentItem;
            },
            
            showReferenceNode : function() {
                me.api.getServer().getReferenceNodeUrl().then(function(url){
                    me.api.setDataUrl(url);
                }, me.renderNotFound);
            },
            
            reload : function() {
                
                // Wait for any reloads currently running
                me.reloadPromise = me.reloadPromise.then(function(ignore, fulfill){
                    var server = me.api.getServer();
                    
                    if( server ) {
                        if( typeof(me.dataUrl) !== "undefined" && me.dataUrl !== null ) {
                            
                            var promise = server.getNodeOrRelationship(me.dataUrl);
                            promise.then(function(item) {
                                me.currentRelatedNodePage = 0;
                                me.currentItem = item;
                                me.render();
                                fulfill();
                            }, function() {
                                me.renderNotFound();
                                fulfill();
                            });
                            
                        } else {
                            me.renderNotFound();
                            fulfill();
                        }
                    } else {
                        me.render();
                        fulfill();
                    }
                });
            }
            
    };
    
    // 
    // PRIVATE
    //
    
    me.reload = me.api.reload;
    
    me.renderNode = function(node, force) {
        
        if(force || me.nodeCache.node !== node ) {
            me.nodeCache.node = node;
            me.nodeCache.relPromise = node.getRelationships();
            me.nodeCache.relNodesPromise = me.reloadRelatedNodes(node);
            me.nodeCache.fieldsPromise = me.nodeCache.relNodesPromise.then(function(nodes, fulfill){
                fulfill(me.propertiesToListManager.setFromNodeList(nodes));
            });
            
            me.nodeCache.allPromise = neo4j.Promise.join(me.nodeCache.relPromise, me.nodeCache.relNodesPromise, me.nodeCache.fieldsPromise); 
        }
        
        me.nodeCache.allPromise.then(function(args) {
            me.processTemplate({
                server : me.server,
                dataUrl : me.dataUrl,
                node : node,
                relationships : args[0],
                relatedNodes : args[1],
                pagination : me.getRelatedNodePagination(args[0]),
                isNode : true,
                relatedFields : me.propertiesToListManager.getListFields()
            });
        });
    };
    
    me.renderRelationship = function(rel) {
        neo4j.Promise.join(rel.getStartNode(), rel.getEndNode()).then(function(nodes){
            me.processTemplate({
                server : me.server,
                dataUrl : me.dataUrl,
                relationship : rel,
                startNode : nodes[0],
                endNode : nodes[1],
                isRelationship : true
            });
        });
    };
    
    me.renderNotFound = function() {
        me.dataUrl = null;
        me.currentItem = null;
        me.processTemplate({
            server : me.server,
            dataUrl : me.dataUrl,
            item : false,
            notFound : true
        });
    };
    
    me.render = function(force) {
        if( me.currentItem instanceof neo4j.models.Node ) {
            me.renderNode(me.currentItem, force);
        } else if( me.currentItem instanceof neo4j.models.Relationship ) {
            me.renderRelationship(me.currentItem, force);
        } else {
            me.renderNotFound();
        }
    };
    
    me.processTemplate = function(ctx) {
        me.processTemplatePromise = me.processTemplatePromise.then(function(ignore, fulfill){
            me.basePage.processTemplate(ctx);
            fulfill();
        });
    };
    
    /**
	 * Fetches all the related nodes for a given node. This is 
	 * used so that we don't have to send one request per node.
	 * 
	 * This will be bad, if there are tons of data. That is, however,
	 * a symptom of the fact that the server does not yet support paging
	 * nodes.
	 * 
	 * @return A promise for a lookup table for nodes.
	 */
    me.reloadRelatedNodes = function(node) {
    	var traversalUrl = node.getSelf() + "/traverse/node";
    	var traversal = {
    		"max depth": 1
    	};
    	
    	var web = wa.Servers.getCurrentServer().web;
        
    	return new neo4j.Promise(function(fulfill, fail){
        	web.post(traversalUrl, traversal, function(data) {
        		// Create a lookup table for related nodes, to make it easy for the
    			// template to render it.
        		var nodes = {};
        		for( var i = 0, l = data.length; i < l; i ++) {
        			nodes[data[i].self] = new neo4j.models.Node(data[i]);
        		}
        		
        		fulfill(nodes);
        	}, fail);
    	});
    };
    
    me.getRelatedNodePagination = function(relationships) {
    	
    	// Pagination pre-calculations
    	var relatedNodeCount = relationships.length,
    	    nodePageCount = Math.ceil(relatedNodeCount / me.relatedNodesPerPage);
    	
    	if(relatedNodeCount > 0) {
    	
        	if(me.currentRelatedNodePage < 0) {
        	    me.currentRelatedNodePage = 0;
        	}
        	
        	if(me.currentRelatedNodePage >= nodePageCount ) {
        	    me.currentRelatedNodePage = nodePageCount - 1;
        	}
        	
        	var relatedNodeStartIndex = me.currentRelatedNodePage * me.relatedNodesPerPage;
        	var relatedNodeStopIndex = relatedNodeStartIndex + me.relatedNodesPerPage - 1;
        	
        	if( relatedNodeStopIndex >= relatedNodeCount ) {
        		relatedNodeStopIndex = relatedNodeCount - 1;
        	}
        	
        	return {
    			relatedNodeStartIndex : relatedNodeStartIndex,
                relatedNodeStopIndex : relatedNodeStopIndex,
                relatedNodePage : me.currentRelatedNodePage,
                relatedNodePageCount : nodePageCount > 0 ? nodePageCount : 1,
                relatedNodesPerPage : me.relatedNodesPerPage
        	};
    	} else {
    	    return {
                relatedNodeStartIndex : 0,
                relatedNodeStopIndex : 0,
                relatedNodePage : 0,
                relatedNodePageCount : 1,
                relatedNodesPerPage : me.relatedNodesPerPage
            };
    	}
    };
    
    /**
	 * This is triggered when the list of names to show when listing nodes
	 * changes.
	 */
    me.listNamesChanged = function(ev) {
    	me.render();
    };
    
    /**
	 * Triggered when the URL hash state changes.
	 */
    me.hashchange = function(ev) {
        var url = $.bbq.getState( "dataurl" );
        
        if( !url ) {
            me.api.showReferenceNode();
        } else {
        	me.dataUrl = url;
        	if( me.uiLoaded && !( me.currentItem && me.currentItem.getSelf() === url )) {
        	    me.reload();
        	}
    	}
    };
    
    //
    // CONSTRUCT
    //
    
    $("a.mor_data_url_button").live("click", function(ev) {
        ev.preventDefault();
        me.api.setDataUrl($(ev.target).attr("href"));
    });
    
    $("button.mor_data_refresh_button").live("click", function(ev){
    	ev.preventDefault();
    	me.render(true);
    });
    
    $("button.mor_data_reference_node_button").live("click", function(ev) {
    	ev.preventDefault();
    	me.api.showReferenceNode();
    });
    
    $("a.mor_data_paginate_next").live("click", function(ev){
    	ev.preventDefault();
    	me.currentRelatedNodePage++;
    	me.render();
    });
    
    $("a.mor_data_paginate_previous").live("click", function(ev){
    	ev.preventDefault();
		me.currentRelatedNodePage--;
		me.render();
    });
    
    function getItemByUrl(ev) {
        ev.preventDefault();
        me.api.setDataUrl($("#mor_data_get_id_input").val());
    }
    
    $("input.mor_data_get_url_button").live("click", getItemByUrl);
    $("#mor_data_get_id_input").live("keydown", function(ev) {
        if(ev.keyCode == 13) {
            getItemByUrl(ev);
        }
    });
    
    return me.api;
    
})(jQuery);

//
// REGISTER STUFF
//

wa.ui.Pages.add("data",wa.components.data.DataBrowser);
wa.ui.MainMenu.add({ label : "Data", pageKey:"data", index:1, perspectives:['server']});

wa.bind("init", wa.components.data.DataBrowser.init);
wa.bind("ui.page.changed", wa.components.data.DataBrowser.pageChanged);
wa.bind("servers.current.changed",  wa.components.data.DataBrowser.serverChanged);