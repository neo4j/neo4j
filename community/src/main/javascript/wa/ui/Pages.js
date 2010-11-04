
/**
 * Morpheus user interface. Builds the base ui, keeps track of registered
 * components and provides an API to inject new UI parts.
 */
wa.ui.Pages = (function($) {

    var me = {};

    // 
    // PRIVATE
    //

    me.DEFAULT_PAGE = "dashboard";

    me.pages = {};
    me.currentPage = null;
    me.pageRoot = null;

    /**
     * Set up the user interface.
     */
    me.init = function() {

        // Init pages
        me.pageRoot = $("#mor_pages");

        for (key in me.pages)
        {
            me.pageRoot.append(me.pages[key].element);
            me.pages[key].element.hide();
        }

        $(window).bind("hashchange", me.hashchange);

        me.showPage($.bbq.getState("p"));

    };

    me.showPage = function(key) {

        // Does the requested page exist?
        if (typeof (me.pages[key]) !== "undefined")
        {
            if (me.currentPage === key)
            {
                return;
            }

            if (me.currentPage !== null)
            {
                me.pages[me.currentPage].element.hide();
            }

            me.currentPage = key;
            me.pages[key].element.show();
            wa.ui.MainMenu.setCurrentPage(key);

            wa.trigger("ui.page.changed", key);
        } else
        {
            if (key !== me.DEFAULT_PAGE)
            {
                $.bbq.pushState({
                    p : me.DEFAULT_PAGE
                });
            } else
            {
                // Default page is not available, try showing the first
                // available page
                for (key in me.pages)
                {
                    $.bbq.pushState({
                        p : key
                    });
                    break;
                }
            }
        }
    };

    /**
     * Called whenever the url hash changes.
     */
    me.hashchange = function(event) {

        var pageKey = $.bbq.getState("p");
        me.showPage(pageKey);

    };

    //
    // PUBLIC INTERFACE
    //

    return {
        init : me.init,
        add : function(key, page) {

            me.pages[key] = {
                obj : page,
                element : page.getPage()
            };

            if (me.pageRoot !== null)
            {
                me.pageRoot.append(me.pages[key].element);
                me.pages[key].element.hide();
            }

        }
    };

})(jQuery);
