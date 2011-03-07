(function() {
  define(function() {
    var FoldoutWatcher;
    return FoldoutWatcher = (function() {
      function FoldoutWatcher() {}
      FoldoutWatcher.prototype.init = function() {
        return $("a.foldout_trigger").live("click", function(ev) {
          ev.preventDefault();
          return $(ev.target).closest(".foldout").toggleClass("visible");
        });
      };
      return FoldoutWatcher;
    })();
  });
}).call(this);
