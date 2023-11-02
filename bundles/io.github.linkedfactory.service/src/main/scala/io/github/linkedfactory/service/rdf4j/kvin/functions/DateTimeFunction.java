package io.github.linkedfactory.service.rdf4j.kvin.functions;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import java.util.Date;
import java.util.GregorianCalendar;

public class DateTimeFunction implements Function {
	@Override
	public String getURI() {
		return "kvin:dateTime";
	}

	@Override
	public Value evaluate(ValueFactory valueFactory, Value... values) throws ValueExprEvaluationException {
		if (values.length > 0 && values[0].isLiteral()) {
			var c = new GregorianCalendar();
			c.setTime(new Date(((Literal) values[0]).longValue()));
			try {
				return valueFactory.createLiteral(DatatypeFactory.newInstance().newXMLGregorianCalendar(c));
			} catch (DatatypeConfigurationException e) {
				throw new ValueExprEvaluationException(e);
			}
		}
		return null;
	}

	@Override
	public boolean mustReturnDifferentResult() {
		return Function.super.mustReturnDifferentResult();
	}
}
