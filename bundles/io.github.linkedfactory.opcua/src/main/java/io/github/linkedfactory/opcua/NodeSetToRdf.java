/*
 * Copyright (c) 2022 Fraunhofer IWU.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.linkedfactory.opcua;

import com.digitalpetri.opcua.nodeset.UaNodeSet;
import com.digitalpetri.opcua.nodeset.attributes.NodeAttributes;
import com.digitalpetri.opcua.nodeset.attributes.VariableNodeAttributes;
import com.digitalpetri.opcua.nodeset.attributes.VariableTypeNodeAttributes;
import com.google.inject.Guice;
import net.enilink.komma.core.*;
import net.enilink.komma.em.concepts.IResource;
import net.enilink.komma.model.*;
import net.enilink.vocab.owl.Class;
import net.enilink.vocab.owl.OWL;
import net.enilink.vocab.rdf.RDF;
import net.enilink.vocab.rdfs.RDFS;
import org.eclipse.milo.opcua.sdk.core.Reference;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExpandedNodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;

import javax.xml.bind.JAXBException;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NodeSetToRdf {
    static final URI UA_NAMESPACE = URIs.createURI("http://opcfoundation.org/UA/");
    static final URI TYPE_DATATYPE = UA_NAMESPACE.appendLocalPart("DataType");
    static final URI PROPERTY_DATATYPE = UA_NAMESPACE.appendLocalPart("dataType");
    static final URI PROPERTY_HASSUBTYPE = UA_NAMESPACE.appendLocalPart("i=45");

    protected UaNodeSet nodeSet;
    protected IModelSet modelSet;
    protected IModel model;

    NodeSetToRdf(UaNodeSet nodeSet) {
        this.nodeSet = nodeSet;

        KommaModule module = ModelPlugin.createModelSetModule(NodeSetToRdf.class.getClassLoader());

        IModelSetFactory factory = Guice.createInjector(new ModelSetModule(module)).getInstance(IModelSetFactory.class);
        modelSet = factory.createModelSet(MODELS.NAMESPACE_URI.appendFragment("MemoryModelSet"));
        // modelSet.getModule().includeModule(new OpcUaModule());

        model = modelSet.createModel(URIs.createURI("http://example.org/opcua/"));
        model.setLoaded(true);
        model.getModelSet().getDataChangeSupport().setEnabled(null, false);
    }

    URI toUri(NodeClass nodeClass) {
        return UA_NAMESPACE.appendLocalPart(nodeClass.name());
    }

    URI toUri(NodeId nodeId) {
        String nsUri = nodeSet.getNamespaceTable().getUri(nodeId.getNamespaceIndex());
        String id = nodeId.toParseableString();
        // remove namespace index
        id = id.replaceFirst("[^;]+;", "");
        return URIs.createURI(nsUri).appendLocalPart(id);
    }

    URI toUri(QualifiedName name) {
        String nsUri = nodeSet.getNamespaceTable().getUri(name.getNamespaceIndex());
        return URIs.createURI(nsUri).appendLocalPart(name.getName());
    }

    /**
     * Converts a nodeset to an RDF representation.
     */
    void convert() {
        final IEntityManager em = model.getManager();
        // mark all node classes as owl:Class
        Stream.of(NodeClass.values()).forEach(nc -> {
            em.createNamed(toUri(nc), OWL.TYPE_CLASS);
        });

        // TODO this seems to be superfluous, check and remove it
        final URI componentProperty = model.getURI().appendLocalPart("component");
        em.createNamed(componentProperty, OWL.TYPE_OBJECTPROPERTY);

        // convert all references contained in the node set to owl:ObjectProperties
        Set<Reference> allRefs = nodeSet.getExplicitReferences().entries().stream().map(e -> e.getValue()).collect(Collectors.toSet());
        allRefs.forEach(ref -> {
            // mark used references as object properties
            URI refUri = toUri(ref.getReferenceTypeId());
            em.add(new Statement(refUri, RDF.PROPERTY_TYPE, OWL.TYPE_OBJECTPROPERTY));
        });

        // convert all the datatype definition to OWL
        nodeSet.getDataTypeDefinitions().forEach((nodeId, dtDefinition) -> {
            URI dtUri = toUri(nodeId);
            // TODO also check reference type
            boolean isEnum = nodeSet.getExplicitReferences().get(nodeId).stream()
                    .filter(ref -> /* HasSubType */ ref.getReferenceTypeId().toParseableString().contains("i=45") &&
                            /* Enum */ ref.getTargetNodeId().toParseableString().contains("i=29"))
                    .findFirst().isPresent();
            Class dtClass = em.createNamed(dtUri, Class.class);
            em.add(new Statement(dtClass, RDFS.PROPERTY_SUBCLASSOF, TYPE_DATATYPE));
            if (isEnum) {
                // maps values of an UA enumeration to owl:oneOf
                List<Object> values = dtDefinition.getField().stream().map(dtField -> {
                    URI valueUri = dtUri.appendQuery(dtField.getName());
                    return valueUri;
                }).collect(Collectors.toList());
                dtClass.setOwlOneOf(values);
            } else {
                // map the fields (attributes) of the data type to OWL
                // TODO mapping rules are missing
                dtDefinition.getField().stream().map(dtField -> {
                    URI property = dtUri.appendQuery(dtField.getName());
                    System.out.println(dtField.getDataType() + " -- " + dtField.getDefinition());
                    return property;
                }).collect(Collectors.toList());

                // mark data type as subclass of the corresponding base type
                Optional.ofNullable(dtDefinition.getBaseType()).filter(bt -> bt.trim().length() > 0).ifPresent(baseType -> {
                    em.add(new Statement(dtClass, RDFS.PROPERTY_SUBCLASSOF, UA_NAMESPACE.appendLocalPart(baseType)));
                });

                System.out.println("DATATYPE: " + dtDefinition.getName() + (dtDefinition.getBaseType() != null ? " extends " + dtDefinition.getBaseType() : ""));
                System.out.println("\t" + dtDefinition.getField().stream().reduce("", (str, field) -> str + (str.isEmpty() ? "" : ", ") + field.getName(), String::concat));
            }
        });

        // convert the individual nodes
        nodeSet.getNodes().entrySet().stream().forEach(e -> {
            NodeId id = e.getKey();
            NodeAttributes attrs = e.getValue();

            // create RDF representation with label and comment
            URI nodeType = toUri(attrs.getNodeClass());
            IResource rdfNode = em.createNamed(toUri(id), nodeType).as(IResource.class);
            rdfNode.setRdfsLabel(attrs.getDisplayName().getText());
            rdfNode.setRdfsComment(attrs.getDescription().getText());
            QualifiedName browseName = attrs.getBrowseName();
            if (browseName != null) {
                rdfNode.addProperty(UA_NAMESPACE.appendLocalPart("browseName"), toUri(browseName));
            }
            switch (attrs.getNodeClass()) {
                case ReferenceType:
                    // mark defined references also as object properties
                    rdfNode.getRdfTypes().add(em.find(OWL.TYPE_OBJECTPROPERTY, Class.class));
                    break;
                case DataType:
                    // mark data types as classes
                    rdfNode.getRdfTypes().add(em.find(OWL.TYPE_CLASS, Class.class));
                    break;
                case Variable: {
                    // add datatype
                    NodeId dataType = ((VariableNodeAttributes) attrs).getDataType();
                    rdfNode.addProperty(PROPERTY_DATATYPE, toUri(dataType));
                    break;
                }
                case VariableType: {
                    // add datatype
                    NodeId dataType = ((VariableTypeNodeAttributes) attrs).getDataType();
                    rdfNode.addProperty(PROPERTY_DATATYPE, toUri(dataType));
                    break;
                }
            }

            // map references to RDF
            boolean nodeIsReference = attrs.getNodeClass() == NodeClass.ReferenceType;
            List<Reference> refs = nodeSet.getExplicitReferences().get(id);
            refs.stream().forEach(ref -> {
                ExpandedNodeId targetIdExpanded = ref.getTargetNodeId();
                targetIdExpanded.local(nodeSet.getNamespaceTable()).ifPresent(targetId -> {
                    IResource rdfTarget = em.find(toUri(targetId), IResource.class);
                    URI refUri = toUri(ref.getReferenceTypeId());
                    if (ref.isForward()) {
                        em.add(new Statement(rdfNode, refUri, rdfTarget));
                    } else {
                        em.add(new Statement(rdfTarget, refUri, rdfNode));
                    }

                    // convert HasSubType to rdfs:subClassOf / rdfs:subPropertyOf
                    if (PROPERTY_HASSUBTYPE.equals(refUri)) {
                        URI property = nodeIsReference ? RDFS.PROPERTY_SUBPROPERTYOF : RDFS.PROPERTY_SUBCLASSOF;
                        if (ref.isForward()) {
                            em.add(new Statement(rdfTarget, property, rdfNode));
                        } else {
                            em.add(new Statement(rdfNode, property, rdfTarget));
                        }
                    }

                    NodeAttributes targetAttrs = nodeSet.getNodes().get(targetId);
                    if (targetAttrs != null) {
                        String type = ref.isForward() ? " -> " : " <- ";
                        System.out.println(attrs.getBrowseName().getName() + " -> " + targetAttrs.getBrowseName().getName());
                    }
                });
            });
        });
    }

    void save(OutputStream os) throws IOException {
        Map<Object, Object> options = new HashMap<>();
        options.put(IModel.OPTION_MIME_TYPE, "text/turtle");
        model.save(os, options);
    }

    public static void main(String[] args) throws JAXBException, IOException {
        if (args.length > 0) {
            Optional<UaNodeSet> nodeSet = Optional.empty();
            try (InputStream is = new FileInputStream(args[0])) {
                nodeSet = Optional.of(UaNodeSet.parse(is));
            } catch (FileNotFoundException e) {
                System.err.println("File '" + args[0] + "' does not exist.");
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (!nodeSet.isPresent()) {
                return;
            }

            NodeSetToRdf nsToRdf = new NodeSetToRdf(nodeSet.get());
            nsToRdf.convert();
            if (args.length > 1) {
                try (FileOutputStream os = new FileOutputStream(args[1])) {
                    nsToRdf.save(os);
                }
            } else {
                nsToRdf.save(System.out);
            }
        } else {
            System.err.println("Usage: NodeSetToRdf <input> [<output>]");
        }
    }
}
