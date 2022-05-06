define([ "flight/lib/component" ], function(defineComponent) {
	return defineComponent(function() {
		this.attributes({});

		this.after('initialize', function() {
			var self = this;
			var property = this.$node.attr("data-property");

			this.on("click", function(evt) {
				self.trigger("graph-setProperties", {
					properties : [ property ]
				});
			});

			this.on(self.$node.closest(".widget"), "graph-setProperties", function(evt, data) {
				$.each(data.properties, function(index, p) {
					if (p == property) {
						self.$node.addClass("active");
					} else {
						self.$node.removeClass("active");
					}
				});
			});
		});
	});
});