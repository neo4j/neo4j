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
            position: ['400',],
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
