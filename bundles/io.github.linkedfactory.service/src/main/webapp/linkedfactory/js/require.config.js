require.config({
	urlArgs : "bust=" + (new Date()).getTime(),
	paths : {
		view : "/linkedfactory/js/view",
		d3 : "/classpath/webjars/d3/7.4.4/dist/d3.min",
		select2 : "/classpath/webjars/select2/4.0.13/dist/js/select2.min",
		moment : "/classpath/webjars/moment/2.29.3/min/moment-with-locales",
		datetimepicker : "/classpath/webjars/eonasdan-bootstrap-datetimepicker/4.17.47/build/js/bootstrap-datetimepicker.min",
		sg : "/classpath/webjars/slickgrid/2.4.9",
		slickgrid : "/classpath/webjars/slickgrid/2.4.9/slick.grid",
		simplify : "/classpath/webjars/simplify-js/1.2.4/simplify"
	},
	shim : {
		'slickgrid' : {
			deps : [ "sg/slick.core", "sg/lib/jquery.event.drag-2.2", "sg/lib/jquery.event.drop-2.2" ],
			exports : "Slick"
		}
	}
});