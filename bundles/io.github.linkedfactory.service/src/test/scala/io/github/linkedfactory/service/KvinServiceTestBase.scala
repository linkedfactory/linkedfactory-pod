package io.github.linkedfactory.service

import io.github.linkedfactory.core.kvin.KvinTuple
import io.github.linkedfactory.core.kvin.util.JsonFormatWriter
import net.enilink.commons.iterator.WrappedIterator
import net.enilink.komma.core.URIs

import java.util

class KvinServiceTestBase {

  def generateJsonFromSingleTuple(): String = {
    val tuple: KvinTuple = new KvinTuple(URIs.createURI("http://example.org/item1"), URIs.createURI("http://example.org/properties/p1"), null, 1619424246120L, 57.934878949512196)
    val json: String = new JsonFormatWriter().toJsonString(WrappedIterator.create(new util.ArrayList[KvinTuple](util.Arrays.asList(tuple)).iterator))
    json
  }

  def generateJsonFromTupleSet(): String = {
    val timeList = Array(1675324136198L, 1675324133196L, 1675324130194L, 1675324127192L, 1675324124192L, 1675324121191L)
    val valueList = Array(6.333333333333333, 6.333333333333333, 6.25, 6.2, 6.166666666666667, 6.166666666666667)
    val tuples: util.ArrayList[KvinTuple] = new util.ArrayList[KvinTuple]()

    for (count <- 1 to 6) {
      tuples.add(new KvinTuple(URIs.createURI("http://example.org/item2"), URIs.createURI("http://example.org/properties/p2"), null, timeList(count - 1), valueList(count - 1)))
    }
    val json: String = new JsonFormatWriter().toJsonString(WrappedIterator.create(tuples.iterator()))
    json
  }
}
