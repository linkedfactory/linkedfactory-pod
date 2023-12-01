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
package io.github.linkedfactory.core.kvin.influxdb

import io.github.linkedfactory.core.kvin.{Kvin, KvinListener, KvinTuple}
import net.enilink.commons.iterator.{IExtendedIterator, NiceIterator, WrappedIterator}
import net.enilink.komma.core.{URI, URIs}
import org.influxdb.InfluxDBFactory
import org.influxdb.dto.{Point, Query}
import org.slf4j.LoggerFactory

import java.util.concurrent.{CopyOnWriteArraySet, TimeUnit}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

class KvinInfluxDb(endpoint: URI, dbName: String) extends Kvin {
  final val log = LoggerFactory.getLogger(classOf[KvinInfluxDb])

  final val dbUser = "root"
  final val dbUserPass = "root"

  val influxdb = InfluxDBFactory.connect(endpoint.toString, dbUser, dbUserPass)
  val ver = influxdb.version()
  log.debug("Connect influxdb-" + ver + " to " + endpoint)

  influxdb.createDatabase(dbName)
  influxdb.enableBatch(500, 50, TimeUnit.MILLISECONDS)

  var knowEntities = TrieMap.empty[(URI, URI), Boolean]

  val listeners = new CopyOnWriteArraySet[KvinListener]

  override def addListener(listener: KvinListener): Boolean = {
    listeners.add(listener)
  }

  override def removeListener(listener: KvinListener): Boolean = {
    listeners.remove(listener)
  }

  override def put(entries: KvinTuple*): Unit = {
    put(entries.asJava)
  }

  override def put(entries: java.lang.Iterable[KvinTuple]): Unit = {
    entries.forEach { tuple =>
      val item = tuple.item
      val property = tuple.property
      val valuePut = tuple.value.toString().toDouble
      val newEntity = if (knowEntities.contains((item, property))) {
        false
      } else {
        val query = "SELECT * FROM \"" + property + "\" WHERE item='" + item + "' LIMIT 1"
        val fromDB = getQuery(query)
        val data = fromDB.getResults.get(0)
        !Option(fromDB.getResults.get(0).getSeries).isEmpty
      }

      val point = Point.measurement(property.toString)
        .time(tuple.time, TimeUnit.MILLISECONDS)
        .tag("item", item.toString)
        .addField("value", valuePut)
        .build
      influxdb.write(dbName, "default", point)

      if (newEntity) {
        knowEntities.put((item, property), true)
        for (l <- listeners.asScala) l.entityCreated(item)
      }
      for (l <- listeners.asScala) l.valueAdded(item, property, tuple.context, tuple.time, 0L, tuple.value)
    }
  }

  def get(property: URI, item: URI, fromTime: Long, toTime: Long) = {
    val query = new Query(s"""SELECT * FROM "$property" WHERE item="$item" AND time <=${toTime * 1000000} AND time >=${fromTime * 1000000}""", dbName)
    val res = influxdb.query(query)
    res.getResults.get(0).getSeries.get(0)
  }

  def getQuery(queryStr: String) = {
    val query = new Query(queryStr, dbName)
    influxdb.query(query, TimeUnit.MILLISECONDS)
  }

  override def delete(item: URI, property: URI, context: URI, end: Long = Long.MaxValue, begin: Long = 0L): Long = {
    val query_del = new Query("DROP SERIES FROM \"" + property + "\" WHERE item='" + item.toString() + "'", dbName)
    val res = influxdb.query(query_del)
    log.debug("Query delete result: {}", res)
    // TODO compute number of deleted items
    1L
  }

  override def delete(item: URI): Boolean = {
    val query_del = new Query("DROP SERIES WHERE item='" + item + "'", dbName)
    val res = influxdb.query(query_del)
    !res.hasError
  }

  override def fetch(item: URI, property: URI, context: URI, limit: Long): IExtendedIterator[KvinTuple] = fetchInternal(item = item, property = property, context = context, limit = limit)

  override def fetch(item: URI, property: URI, context: URI, end: Long = KvinTuple.TIME_MAX_VALUE, begin: Long = 0L, limit: Long = 0L, interval: Long = 0L, op: String = null): IExtendedIterator[KvinTuple] = {
    fetchInternal(item, property, context, end, begin, limit, interval)
  }

  def fetchInternal(item: URI, property: URI, context: URI, end: Long = KvinTuple.TIME_MAX_VALUE, begin: Long = 0L, limit: Long = 0L, interval: Long = 0L, op: String = null): IExtendedIterator[KvinTuple] = {
    var endTime = end
    if (endTime == Long.MaxValue) endTime = Long.MaxValue / 1000000
    val q = if (interval != 0L) {
      "SELECT MEAN(value) FROM \"" + property + "\" WHERE item='" + item + "' AND time<=" + endTime * 1000000 + " AND time>=" + begin * 1000000 + " GROUP BY time(" + interval + "ms) fill(none) LIMIT " + limit
    } else {
      "SELECT value FROM \"" + property + "\" WHERE item='" + item + "' AND time<=" + endTime * 1000000 + " AND time>=" + begin * 1000000 + " LIMIT " + limit
    }
    val query = q
    val fromDB = getQuery(query)
    log.debug("Query result from fetch: {}", fromDB)
    val data = fromDB.getResults.get(0).getSeries
    if (!data.isEmpty) {
      val it = data.get(0).getValues.iterator.asScala.map { e => new KvinTuple(item, property, context, e.get(0).toString().toDouble.toLong, 0, e.get(1)) }
      WrappedIterator.create(it.asJava)
    } else NiceIterator.emptyIterator[KvinTuple]
  }

  override def properties(item: URI): IExtendedIterator[URI] = {
    val query = "SHOW SERIES WHERE item = '" + item.toString() + "'"
    val fromDB = getQuery(query)
    log.debug("Property found: {} ", fromDB)
    val opt = fromDB.getResults.get(0).getSeries
    if (!opt.isEmpty) {
      val data = opt.get(0).getValues
      val it = data.iterator.asScala.map(e => URIs.createURI(e.get(0).toString().split(",")(0)))
      WrappedIterator.create(it.asJava)
    } else NiceIterator.emptyIterator[URI]
  }

  override def descendants(uri: URI): IExtendedIterator[URI] = descendants(uri, Long.MaxValue)

  override def descendants(item: URI, limit: Long = Long.MaxValue): IExtendedIterator[URI] = {
    val query = "SHOW TAG VALUES WITH key = item"
    val fromDB = getQuery(query)
    var res = new ListBuffer[URI]
    val opt = Option(fromDB.getResults.get(0).getSeries)
    if (!opt.isEmpty) {
      val data = fromDB.getResults.get(0).getSeries
      val iteratorItem = data.iterator()
      while (iteratorItem.hasNext()) {
        val curr = iteratorItem.next()
        val dbitem = curr.getValues.iterator
        while (dbitem.hasNext()) {
          val name = dbitem.next().get(1)
          if (name.toString().startsWith(item.toString())) {
            val uri = URIs.createURI(name.toString())
            res += uri
          }
        }
      }
    }
    WrappedIterator.create(res.toList.iterator.asJava)
  }

  override def approximateSize(item: URI, property: URI, context: URI, end: Long = Long.MaxValue / 1000000, begin: Long = 0L): Long = {
    val query = ("SELECT count(value) FROM \"" + property + "\" WHERE item='" + item + "' AND time <=" + end * 1000000 + " AND time >=" + begin * 1000000)
    val fromDB = getQuery(query)
    fromDB.getResults.get(0).getSeries.get(0).getValues.get(0).get(1).toString().toDouble.toLong
  }

  override def close {
    if (influxdb.isBatchEnabled) influxdb.disableBatch
  }
} 