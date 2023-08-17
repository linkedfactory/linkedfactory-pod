package io.github.linkedfactory.opcua;

import com.digitalpetri.opcua.nodeset.UaNodeSet;
import org.junit.Test;

import javax.xml.bind.JAXBException;

public class NodeSetToRdfTest {
	@Test
	public void simpleTest() throws JAXBException {
		UaNodeSet nodeSet = UaNodeSet.parse(getClass().getResourceAsStream("/Opc.ISA95.NodeSet2.xml"));
		NodeSetToRdf nsToRdf = new NodeSetToRdf(nodeSet);
		nsToRdf.convert();
	}
}
