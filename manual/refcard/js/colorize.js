CodeMirror.colorize = (function() {

  var isBlock = /^(p|li|div|h\\d|pre|blockquote|td)$/;

  function textContent(node, out) {
    if (node.nodeType == 3) return out.push(node.nodeValue);
    for (var ch = node.firstChild; ch; ch = ch.nextSibling) {
      textContent(ch, out);
      if (isBlock.test(node.nodeType)) out.push("\n");
    }
  }

  return function() {
    var collection = document.body.getElementsByTagName("pre");

    var theme = " cm-s-neo";
    for (var i = 0; i < collection.length; ++i) {
      var node = collection[i];

      var text = [];
      textContent(node, text);
      node.innerHTML = "";
      CodeMirror.runMode(text.join(""), "cypher", node);

      node.className += theme;
    }
  };
})();

