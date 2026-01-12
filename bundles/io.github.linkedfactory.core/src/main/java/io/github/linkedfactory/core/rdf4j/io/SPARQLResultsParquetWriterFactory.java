package io.github.linkedfactory.core.rdf4j.io;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultFormat;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriter;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriterFactory;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class SPARQLResultsParquetWriterFactory implements TupleQueryResultWriterFactory {
	static final IRI SPARQL_RESULTS_PARQUET_URI = SimpleValueFactory.getInstance()
			.createIRI("http://www.w3.org/ns/formats/SPARQL_Results_Parquet");
	static final TupleQueryResultFormat SPARQL_PARQUET = new TupleQueryResultFormat(
			"SPARQL/Parquet", List.of("application/vnd.apache.parquet"), StandardCharsets.UTF_8,
			List.of("parquet"), SPARQL_RESULTS_PARQUET_URI, false);

	@Override
	public TupleQueryResultFormat getTupleQueryResultFormat() {
		return SPARQL_PARQUET;
	}

	@Override
	public TupleQueryResultWriter getWriter(OutputStream outputStream) {
		return new SPARQLResultsParquetWriter(outputStream);
	}
}
