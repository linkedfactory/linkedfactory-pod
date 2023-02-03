package io.github.linkedfactory.kvin.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.linkedfactory.kvin.KvinTuple;
import io.github.linkedfactory.kvin.Record;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonFormatWriter {
    ObjectMapper mapper;
    public JsonFormatWriter() {
        mapper = new ObjectMapper();
    }
    public String toJsonString(IExtendedIterator<KvinTuple> tuples) {
        try {
            // grouping
            Map<URI, Map<URI, List<KvinTuple>>> groupedData = new HashMap<>();
            for (KvinTuple tuple : tuples) {
                Map<URI, List<KvinTuple>> propertyData = groupedData.computeIfAbsent(tuple.item, (item) -> new HashMap<>());
                List<KvinTuple> values = propertyData.computeIfAbsent(tuple.property, (property) -> new ArrayList<>());
                values.add(tuple);
            }

            // converting tuples to json
            ObjectNode rootNode = mapper.createObjectNode();

            for (Map.Entry<URI, Map<URI, List<KvinTuple>>> data : groupedData.entrySet()) {
                ObjectNode predicateNode = mapper.createObjectNode();
                for (Map.Entry<URI, List<KvinTuple>> property : data.getValue().entrySet()) {
                    ArrayList<ObjectNode> objectList = new ArrayList<>();
                    for (KvinTuple tuple : property.getValue()) {
                        ObjectNode objectNode = mapper.createObjectNode();
                        objectNode.put("value", objectToJson(tuple.value));
                        objectNode.put("time", tuple.time);
                        objectNode.put("seqNr", tuple.seqNr);
                        objectList.add(objectNode);
                    }
                    predicateNode.set(property.getKey().toString(), mapper.createArrayNode().addAll(objectList));
                }
                rootNode.set(data.getKey().toString(), predicateNode);
            }

            String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootNode);
            return json;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private JsonNode objectToJson(Object object) {
        JsonNode rootNode;

        if (object instanceof Record) {
            ObjectNode node = mapper.createObjectNode();
            node.set(((Record) object).getProperty().toString(), mapper.valueToTree(objectToJson(((Record) object).getValue())));
            rootNode = node;
        } else {
            rootNode = mapper.valueToTree(object);
        }

        //handling id -> @id conversion
        if (!rootNode.path("id").isMissingNode() && !rootNode.isTextual()) {
            ObjectNode node = (ObjectNode) rootNode;
            node.set("@id", rootNode.get("id"));
            node.remove("id");
        }
        return rootNode;
    }
}
