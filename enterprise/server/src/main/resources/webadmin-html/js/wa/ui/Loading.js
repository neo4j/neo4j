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
 * Shows a loading message that blocks the UI.
 */
wa.ui.Loading = (function($){
    var me = {};
    
    me.container = null;
    
    me.showImpl = function() {
        $("#mor_loading_content").modal({
            overlayId: 'mor_loading_overlay',
            containerId: 'mor_loading_container',
            closeHTML: null,
            minHeight: 80,
            opacity: 65, 
            position: ['100',],
            overlayClose: false
        });
    };
    
    return {
        show : function(title, message, cb) {
            me.cb = cb;
            $("#mor_loading_title").html(title);
            $("#mor_loading_message").html(message);
            me.showImpl();
        },
        
        hide : function() {
            $.modal.close();
        }
    };
})(jQuery);
