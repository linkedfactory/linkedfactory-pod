package io.github.linkedfactory.core.rdf4j.io;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryResultHandlerException;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.resultio.AbstractQueryResultWriter;
import org.eclipse.rdf4j.query.resultio.QueryResultFormat;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultFormat;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriter;
import org.eclipse.rdf4j.sail.shacl.ast.planNodes.SingletonBindingSet;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class SPARQLResultsParquetWriter extends AbstractQueryResultWriter implements TupleQueryResultWriter {
	final OutputStream out;
	final OutputFile outFile;

	List<String> bindingNames;
	ParquetWriter<BindingSet> parquetWriter;

	SPARQLResultsParquetWriter(OutputStream out) {
		this.out = out;
		this.outFile = new OutputFileOutputStream(new BufferedOutputStream(out));
	}

	@Override
	public TupleQueryResultFormat getTupleQueryResultFormat() {
		return SPARQLResultsParquetWriterFactory.SPARQL_PARQUET;
	}

	@Override
	public QueryResultFormat getQueryResultFormat() {
		return SPARQLResultsParquetWriterFactory.SPARQL_PARQUET;
	}

	@Override
	public void handleNamespace(String s, String s1) throws QueryResultHandlerException {
	}

	@Override
	public void startDocument() throws QueryResultHandlerException {
	}

	@Override
	public void startQueryResult(List<String> bindingNames) throws TupleQueryResultHandlerException {
		this.bindingNames = bindingNames;
		super.startQueryResult(bindingNames);
	}

	protected void handleSolutionImpl(BindingSet bindings) throws TupleQueryResultHandlerException {
		if (parquetWriter == null) {
			try {
				this.parquetWriter = BindingSetParquetWriter.builder(this.outFile)
						.withBindingNames(this.bindingNames)
						.withBindingSet(bindings)
						.build();
			} catch (IOException e) {
				throw new TupleQueryResultHandlerException(e);
			}
		}
		try {
			this.parquetWriter.write(bindings);
		} catch (IOException e) {
			throw new TupleQueryResultHandlerException(e);
		}
	}

	@Override
	public void handleStylesheet(String s) throws QueryResultHandlerException {
	}

	@Override
	public void startHeader() throws QueryResultHandlerException {
	}

	@Override
	public void endHeader() throws QueryResultHandlerException {
	}

	@Override
	public void handleBoolean(boolean b) throws QueryResultHandlerException {
		handleSolutionImpl(new SingletonBindingSet("result",
				SimpleValueFactory.getInstance().createLiteral(b)));
	}

	@Override
	public void handleLinks(List<String> list) throws QueryResultHandlerException {
	}

	@Override
	public void endQueryResult() throws TupleQueryResultHandlerException {
		if (this.parquetWriter != null) {
			try {
				this.parquetWriter.close();
			} catch (IOException e) {
				throw new TupleQueryResultHandlerException(e);
			}
		}
		try {
			this.out.close();
		} catch (IOException e) {
			throw new TupleQueryResultHandlerException(e);
		}
	}
}
