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
package io.github.linkedfactory.core.kvin.leveldb

import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.util.concurrent.Striped
import io.github.linkedfactory.core.kvin._
import io.github.linkedfactory.core.kvin.util.{AggregatingIterator, Values, Varint}
import net.enilink.commons.iterator.{IExtendedIterator, NiceIterator, UniqueExtendedIterator, WrappedIterator}
import net.enilink.komma.core.{URI, URIs}
import org.iq80.leveldb.impl.Iq80DBFactory.{bytes, factory}
import org.iq80.leveldb.{CompressionType, DB, Options, Range, WriteBatch, WriteOptions}

import java.io.{ByteArrayOutputStream, File, IOException, UncheckedIOException}
import java.nio.{ByteBuffer, ByteOrder}
import java.{io, util}
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.locks.{ReadWriteLock, ReentrantReadWriteLock}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

sealed trait EntryType {
  def id: Int

  def reverse: Int = id + 1
}

object EntryType {
  object SubjectToId extends EntryType {
    val id = 1
  }

  object PropertyToId extends EntryType {
    val id = 3
  }

  object ResourceToId extends EntryType {
    val id = 5
  }

  object ContextToId extends EntryType {
    val id = 7
  }
}

object SCPtoId extends EntryType {
  val id = 9
}

/**
 * Indirect mapping of (item, property) -> ID and (ID, time, sequence-nr) -> value.
 */
class KvinLevelDb(path: File) extends KvinLevelDbBase with Kvin {
  val idKey: Array[Byte] = bytes("__NEXT_ID\u0000")

  val locks: Striped[ReadWriteLock] = Striped.readWriteLock(64)

  val uriToIdCache: Cache[(URI, Int), Array[Byte]] = CacheBuilder.newBuilder.maximumSize(20000).build[(URI, Int), Array[Byte]]
  val scpToIdCache: Cache[(URI, URI, URI), Array[Byte]] = CacheBuilder.newBuilder.maximumSize(20000).build[(URI, URI, URI), Array[Byte]]

  // open the LevelDB instance
  def createOptions(timeSeries: Boolean): Options = {
    val options = new Options
    options.createIfMissing(true)
    // set compression type
    options.compressionType(CompressionType.SNAPPY)
    // can be increased for write performance
    options.writeBufferSize(options.writeBufferSize * 4)

    // default is 4096
    options.blockSize(4096 * 2)

    if (timeSeries) {
      // default is 16
      options.blockRestartInterval(64)
      //      options.reverseOrdering(true)
      //      options.timeSeriesMode(true)
    }

    options
  }

  val ids: DB = factory.open(new File(path, "ids"), createOptions(false))
  val values: DB = factory.open(new File(path, "values"), createOptions(true))
  val listeners = new CopyOnWriteArraySet[KvinListener]

  def getIdStore(): DB = ids

  def getValueStore(): DB = values

  def getEntryTypeObj() = EntryType

  override def addListener(listener: KvinListener): Boolean = {
    listeners.add(listener)
  }

  override def removeListener(listener: KvinListener): Boolean = {
    listeners.remove(listener)
  }

  var id: Long = readLong(ids, idKey)
  if (id <= 0) id = 1L

  private def nextId = {
    val result = id
    id += 1
    putLong(ids, idKey, id)
    result
  }

  def uriKey(prefix: Byte, uri: URI): Array[Byte] = {
    val uriBytes = uri.toString.getBytes("UTF-8")
    // append 0 after the uri to ensure that it is not a prefix of another string
    val key = new Array[Byte](uriBytes.length + 2)
    key(0) = prefix
    System.arraycopy(uriBytes, 0, key, 1, uriBytes.length)
    key
  }

  def idKey(prefix: Byte, id: Array[Byte]): Array[Byte] = {
    val key = new Array[Byte](id.length + 1)
    key(0) = prefix
    System.arraycopy(id, 0, key, 1, id.length)
    key
  }

  protected def toId(uri: URI, entryType: EntryType, generate: Boolean): Array[Byte] = {
    val cacheKey = (uri, entryType.id)
    var idBytes = uriToIdCache.getIfPresent(cacheKey)
    if (idBytes == null) {
      val key = uriKey(entryType.id.toByte, uri)
      idBytes = ids.get(key)
      if (idBytes == null && generate) {
        val lock = lockFor(uri)
        writeLock(lock) {
          idBytes = ids.get(key)
          val createNew = idBytes == null
          if (createNew) {
            val id = nextId
            idBytes = new Array[Byte](Varint.calcLengthUnsigned(id))
            Varint.writeUnsigned(idBytes, 0, id)
            val batch = ids.createWriteBatch()
            // add forward mapping
            batch.put(key, idBytes)
            // add reverse mapping
            val idKeyBytes = idKey(entryType.reverse.toByte, idBytes)
            batch.put(idKeyBytes, uri.toString.getBytes("UTF-8"))

            // Ensure that writes to the id database are always synced to disk.
            // As ids are subject to fewer changes the pages may only
            // be flushed with large delays to disk which may cause data loss.
            ids.write(batch, new WriteOptions().sync(true))

            if (entryType == EntryType.SubjectToId) for (l <- listeners.asScala) l.entityCreated(uri)
          }
        }
      }
      if (idBytes != null) {
        uriToIdCache.put(cacheKey, idBytes)
      }
    }
    idBytes
  }

  def deleteId(uri: URI, entryType: EntryType): Unit = {
    val lock = lockFor(uri)
    writeLock(lock) {
      val key = uriKey(entryType.id.toByte, uri)
      ids.delete(key)
    }
  }

  def deleteId(item: URI, property: URI, context: URI): Unit = {
    val lock = lockFor(item)
    writeLock(lock) {
      val key = toId(item, property, context, generate = false)
      ids.delete(key)

      // TODO also delete inverse indexes etc.
    }
  }

  def contextOrDefault(context: URI): URI = if (context == null) Kvin.DEFAULT_CONTEXT else context

  def toId(item: URI, property: URI, context: URI, generate: Boolean): Array[Byte] = {
    val cacheKey = (item, context, property)
    var id = scpToIdCache.getIfPresent(cacheKey)
    if (id == null) {
      val itemId = toId(item, EntryType.SubjectToId, generate)
      if (itemId != null) {
        val contextId = toId(contextOrDefault(context), EntryType.ContextToId, generate)
        if (contextId != null) {
          val propertyId = toId(property, EntryType.PropertyToId, generate)
          if (propertyId != null) {
            id = new Array[Byte](contextId.length + itemId.length + propertyId.length)

            System.arraycopy(itemId, 0, id,0, itemId.length)
            System.arraycopy(contextId, 0, id, itemId.length, contextId.length)
            System.arraycopy(propertyId, 0, id, itemId.length + contextId.length, propertyId.length)

            scpToIdCache.put(cacheKey, id)
          }
          id
        } else null
      } else null
    } else id
  }

  def toUri(id: Array[Byte], entryType: EntryType): Option[URI] = {
    val uriBytes = ids.get(idKey(entryType.reverse.toByte, id))
    if (uriBytes == null) None else Some(URIs.createURI(new String(uriBytes, "UTF-8")))
  }

  override def delete(item: URI, context: URI): Boolean = {
    val BATCH_SIZE = 100000
    val contextId = toId(if (context == null) Kvin.DEFAULT_CONTEXT else context, EntryType.ContextToId, false)
    val itemId = toId(item, EntryType.SubjectToId, generate = false)
    var deletedAny = false
    if (itemId != null && contextId != null) {
      val prefix = new Array[Byte](itemId.length + contextId.length)
      System.arraycopy(itemId, 0, prefix, 0, itemId.length)
      System.arraycopy(contextId, 0, prefix, itemId.length, contextId.length)

      val it = values.iterator
      try {
        var batch: WriteBatch = null
        var count = 0L
        var done = false

        it.seek(prefix)
        while (it.hasNext && !done) {
          val entry = it.next
          val key = entry.getKey
          if (key.startsWith(prefix)) {
            deletedAny = true

            if (batch == null) batch = values.createWriteBatch
            count += 1
            batch.delete(key)
            if (count % BATCH_SIZE == 0) {
              values.write(batch, new WriteOptions().sync(false))
              batch.close()
              batch = values.createWriteBatch
            }
          } else done = true
        }

        if (batch != null && count % BATCH_SIZE != 0) {
          values.write(batch, new WriteOptions().sync(false))
        }

        it.seek(itemId)
        if (! it.hasNext) {
          // no more values for item in the given context exist
          deleteId(item, EntryType.SubjectToId)
        }
      } finally {
        it.close()
      }
    }
    deletedAny
  }

  override def delete(item: URI, property: URI, context: URI,
                      end: Long = KvinTuple.TIME_MAX_VALUE, begin: Long = 0L): Long = {
    val lock = lockFor(item)
    readLock(lock) {
      val BATCH_SIZE = 100000
      val id = toId(item, property, context, generate = false)
      if (id == null) 0L else {
        val idTimePrefix = new Array[Byte](id.length + Varint.MAX_BYTES)
        val prefixBuffer = ByteBuffer.wrap(idTimePrefix).order(BYTE_ORDER)
        prefixBuffer.put(id)
        writeVarint(prefixBuffer, end)
        var batch: WriteBatch = null
        var count = 0L
        val it = values.iterator
        try {
          var done = false
          it.seek(idTimePrefix)
          while (it.hasNext && !done) {
            val entry = it.next
            val key = entry.getKey
            if (key.startsWith(id)) {
              val bb = ByteBuffer.wrap(key, id.length, key.length - id.length).order(BYTE_ORDER)
              val time = readVarint(bb)
              if (time <= end && time >= begin) {
                if (batch == null) batch = values.createWriteBatch
                count += 1
                batch.delete(key)
                if (count % BATCH_SIZE == 0) {
                  values.write(batch, new WriteOptions().sync(false))
                  batch.close()
                  batch = values.createWriteBatch
                }
              } else done = true
            } else done = true
          }
          if (batch != null && count % BATCH_SIZE != 0) {
            values.write(batch, new WriteOptions().sync(false))
          }
        } finally {
          it.close()
          if (batch != null) batch.close()
        }
        // this needs to be a new iterator else some values are found even if all where deleted before (bug?!)
        val checkValuesIt = values.iterator
        try {
          writeLock(lock) {
            // test if some other values exist for this item and property
            checkValuesIt.seek(idTimePrefix)
            val someValuesExist = checkValuesIt.hasNext && checkValuesIt.next.getKey.startsWith(id)
            // if no values exist then delete the corresponding ID
            if (!someValuesExist) deleteId(item, property, context)
          }
          count
        } finally {
          checkValuesIt.close()
          if (batch != null) batch.close()
        }
      }
    }
  }

  override def descendants(uri: URI, context: URI): IExtendedIterator[URI] = descendants(uri, context, Long.MaxValue)

  override def descendants(uri: URI, context: URI, limit: Long): IExtendedIterator[URI] = entries(EntryType.SubjectToId, uri, context, limit)

  def entries(entryType: EntryType, uriPrefix: URI, context: URI, limit: Long): IExtendedIterator[URI] = {
    val contextId = toId(if (context == null) Kvin.DEFAULT_CONTEXT else context, EntryType.ContextToId, false)
    if (contextId == null) {
      NiceIterator.emptyIterator()
    } else {
      val uriBytes = uriPrefix.toString.getBytes("UTF-8")
      val prefix = new Array[Byte](uriBytes.length + 1)
      prefix(0) = entryType.id.toByte
      System.arraycopy(uriBytes, 0, prefix, 1, uriBytes.length)

      val valuesPrefix = new Array[Byte]((LONG_BYTES + 1) * 2)

      val it = ids.iterator
      val valuesIt = values.iterator
      var count = 0L
      new StoreIterator[URI](it) {
        lazy val seen = mutable.Set.empty[URI]

        override def init(): Unit = {
          it.seek(prefix)
        }

        override def computeNext: Option[URI] = {
          val entry = it.next
          val key = entry.getKey
          var done = false
          var result: Option[URI] = None
          while (it.hasNext && !done) {
            if (key.startsWith(prefix) && count < limit) {
              val itemId = entry.getValue
              System.arraycopy(itemId, 0, valuesPrefix, 0, itemId.length)
              System.arraycopy(contextId, 0, valuesPrefix, itemId.length, contextId.length)
              util.Arrays.fill(valuesPrefix, itemId.length + contextId.length, valuesPrefix.length, 0.asInstanceOf[Byte])

              // test if there is an entry with the given context in the values table
              valuesIt.seek(valuesPrefix)
              if (valuesIt.hasNext() &&
                util.Arrays.compare(valuesIt.next().getKey(), 0, itemId.length + contextId.length,
                  valuesPrefix, 0, itemId.length + contextId.length) == 0) {

                // create array of character bytes without prefix and without trailing 0
                val uriBytes = new Array[Byte](key.indexOf(0) - 1)
                System.arraycopy(key, 1, uriBytes, 0, uriBytes.length)
                val uri = URIs.createURI(new String(uriBytes, "UTF-8"))
                if (seen.add(uri)) {
                  count += 1
                  result = Some(uri)
                  done = true
                }
              }
            } else done = true
          }
          result
        }

        override def close(): Unit = {
            try {
              if (open) valuesIt.close()
            } finally {
              super.close()
            }
        }
      }
    }
  }

  override def properties(item: URI, context: URI): IExtendedIterator[URI] = {
    val contextId = toId(if (context == null) Kvin.DEFAULT_CONTEXT else context, EntryType.ContextToId, generate = false)
    val itemId = toId(item, EntryType.SubjectToId, generate = false)
    if (contextId == null || itemId == null) NiceIterator.emptyIterator[URI] else {
      val itemAndContextId = new Array[Byte](contextId.length + itemId.length)
      System.arraycopy(itemId, 0, itemAndContextId, 0, itemId.length)
      System.arraycopy(contextId, 0, itemAndContextId, itemId.length, contextId.length)

      val prefix = new Array[Byte](itemId.length + contextId.length + java.lang.Long.BYTES + 1)
      System.arraycopy(itemId, 0, prefix, 0, itemId.length)
      System.arraycopy(contextId, 0, prefix, itemId.length, contextId.length)

      val it = values.iterator
      var propertyId: Long = 0
      UniqueExtendedIterator.create(
        new StoreIterator[URI](it) {
          override def init(): Unit = {
            it.seek(prefix)
          }

          override def computeNext: Option[URI] = {
            val entry = it.next
            val key = entry.getKey
            if (key.startsWith(itemAndContextId)) {
              val propertyIdLength = varIntLength(key, itemId.length + contextId.length)
              val propertyIdBytes = new Array[Byte](propertyIdLength)
              System.arraycopy(key, itemId.length + contextId.length, propertyIdBytes, 0, propertyIdBytes.length)
              propertyId = Varint.readUnsigned(ByteBuffer.wrap(propertyIdBytes))
              Varint.writeUnsigned(prefix, contextId.length + itemId.length, propertyId + 1)
              // go to next property
              it.seek(prefix)
              toUri(propertyIdBytes, EntryType.PropertyToId)
            } else None
          }
        })
    }
  }

  /**
   * Returns all tuples in this store
   *
   * @return iterator with all tuples
   */
  def fetchAll(): IExtendedIterator[KvinTuple] = {
    val it = values.iterator
    var item: URI = null
    var property: URI = null
    var context: URI = null
    var itemId: Long = 0
    var propertyId: Long = 0
    var contextId: Long = 0
    new StoreIterator[KvinTuple](it) {
      def subArray(key: Array[Byte], index: Int, length: Int): Array[Byte] = {
        val sub = new Array[Byte](length)
        System.arraycopy(key, index, sub, 0, length)
        sub
      }

      override def computeNext: Option[KvinTuple] = {
        var next: KvinTuple = null
        while (next == null && it.hasNext) {
          val entry = it.next
          val key = entry.getKey
          val keyBb = ByteBuffer.wrap(key)

          val itemIdLength = Varint.firstToLength(keyBb.get(keyBb.position()))
          val newItemId = Varint.readUnsigned(keyBb)
          if (newItemId != itemId) {
            item = toUri(subArray(key, 0, itemIdLength), EntryType.SubjectToId).getOrElse(null)
            itemId = newItemId
          }
          val contextIdLength = Varint.firstToLength(keyBb.get(keyBb.position()))
          val newContextId = Varint.readUnsigned(keyBb)
          if (newContextId != contextId) {
            context = toUri(subArray(key, itemIdLength, contextIdLength), EntryType.ContextToId).getOrElse(null)
            contextId = newContextId
          }
          val propertyIdLength = Varint.firstToLength(keyBb.get(keyBb.position()))
          val newPropertyId = Varint.readUnsigned(keyBb)
          if (newPropertyId != propertyId) {
            property = toUri(subArray(key, itemIdLength + contextIdLength, propertyIdLength), EntryType.PropertyToId).getOrElse(null)
            propertyId = newPropertyId
          }

          if (item != null && property != null && context != null) {
            val time = readVarint(keyBb)
            val seq: Int = if (keyBb.hasRemaining) readVarint(keyBb).toInt else 0
            next = new KvinTuple(item, property, context, time, seq, decode(entry.getValue))
          }
        }
        Option(next)
      }
    }
  }

  def lockFor[T](uri: URI): ReentrantReadWriteLock = locks.get(uri).asInstanceOf[ReentrantReadWriteLock]

  def writeLock[T](lock: ReentrantReadWriteLock)(block: => T): T = {
    var readLock = false
    try {
      if (lock.getReadHoldCount > 0) {
        readLock = true
        lock.readLock.unlock()
      }
      lock.writeLock.lock()
      block
    } finally {
      lock.writeLock.unlock()
      if (readLock) lock.readLock.lock()
    }
  }

  def readLock[T](lock: ReadWriteLock)(block: => T): T = try {
    lock.readLock.lock();
    block
  } finally {
    lock.readLock.unlock()
  }

  override def put(entries: KvinTuple*): Unit = {
    if (entries.length > 5) {
      // write in batch
      put(entries.asJava)
    } else {
      // write directly
      entries.foreach { entry => // encode value first to circumvent problems with locks
        val encodedValue = encode(entry.value)
        val lock = lockFor(entry.item)
        readLock(lock) {
          val prefix = toId(entry.item, entry.property, entry.context, generate = true)
          val key = new Array[Byte](prefix.length + Varint.calcLengthUnsigned(entry.time) +
            Varint.calcLengthUnsigned(entry.seqNr))
          val bb = ByteBuffer.wrap(key).order(BYTE_ORDER)
          bb.put(prefix)
          writeVarint(bb, entry.time)
          writeVarint(bb, entry.seqNr)

          values.put(key, encodedValue)

          // remove timed-out entries
          ttl(entry.item) map (asyncRemoveByTtl(values, prefix, _))
        }
      }
      entries.foreach { entry =>
        for (l <- listeners.asScala) l.valueAdded(entry.item, entry.property, entry.context, entry.time, entry.seqNr, entry.value)
      }
    }
  }

  override def put(entries: java.lang.Iterable[KvinTuple]): Unit = {
    val batch = values.createWriteBatch
    try {
      entries.asScala.foreach { entry => // encode value first to circumvent problems with locks
        val encodedValue = encode(entry.value)
        val lock = lockFor(entry.item)
        readLock(lock) {
          val prefix = toId(entry.item, entry.property, entry.context, generate = true)
          val key = new Array[Byte](prefix.length + Varint.calcLengthUnsigned(entry.time) +
            Varint.calcLengthUnsigned(entry.seqNr))
          val bb = ByteBuffer.wrap(key).order(BYTE_ORDER)
          bb.put(prefix)
          writeVarint(bb, entry.time)
          writeVarint(bb, entry.seqNr)

          batch.put(key, encodedValue)

          // remove timed-out entries
          ttl(entry.item) map (asyncRemoveByTtl(values, prefix, _))
        }
      }
      values.write(batch)
    } finally {
      batch.close()
    }
    entries.asScala.foreach { entry =>
      for (l <- listeners.asScala) l.valueAdded(entry.item, entry.property, entry.context, entry.time, entry.seqNr, entry.value)
    }
  }

  override def fetch(item: URI, property: URI, context: URI, limit: Long): IExtendedIterator[KvinTuple] = fetchInternal(item = item, property = property, context = context, limit = limit)

  override def fetch(item: URI, property: URI, context: URI, end: Long = KvinTuple.TIME_MAX_VALUE, begin: Long = 0L, limit: Long = 0L, interval: Long = 0L, op: String = null): IExtendedIterator[KvinTuple] = {
    var results = fetchInternal(item, property, context, end, begin, if (op == null) limit else 0L, if (op == null) interval else 0L)
    if (op != null) {
      results = new AggregatingIterator(results, interval, op.trim.toLowerCase, limit) {
        override def createElement(item: URI, property: URI, context: URI, time: Long, seqNr: Int, value: Object): KvinTuple = {
          new KvinTuple(item, property, context, time, seqNr, value)
        }
      }
    }
    results
  }

  def encode(value: Any): Array[Byte] = {
    value match {
      case d: Data[_] =>
        val baos = new ByteArrayOutputStream
        try {
          // marker for an object
          baos.write('O'.toByte)
          val size = d.size()
          val length: Array[Byte] = Array.ofDim(Varint.calcLengthUnsigned(size))
          Varint.writeUnsigned(length, 0, size)
          baos.write(length)
          for {
            element <- d.asScala
          } {
            // write the property
            val p = element.getProperty
            val pId = toId(p, EntryType.PropertyToId, generate = true)
            baos.write(pId)

            // write the value
            baos.write(encode(element.getValue))
          }
          baos.toByteArray
        } finally {
          baos.close()
        }
      case a: Array[_] =>
        val baos = new ByteArrayOutputStream
        try {
          // marker for an array
          baos.write('['.toByte)
          val length: Array[Byte] = Array.ofDim(Varint.calcLengthUnsigned(a.length))
          Varint.writeUnsigned(length, 0, a.length)
          baos.write(length)
          a.foreach(e => baos.write(encode(e)))
          baos.toByteArray
        } finally {
          baos.close()
        }
      case ref: URI =>
        val refId = toId(ref, EntryType.ResourceToId, generate = true)
        val refData = new Array[Byte](1 + refId.length)
        refData(0) = 'R'.toByte
        System.arraycopy(refId, 0, refData, 1, refId.length)
        refData
      case _ => Values.encode(value match {
        case bi: BigInt => bi.bigInteger
        case bd: BigDecimal => bd.bigDecimal
        case other => other
      })
    }
  }

  def decode(data: Array[Byte]): Any = decode(ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN))

  def decode(b: ByteBuffer): Any = {
    val oldPos = b.position()
    b.get match {
      // this is an object
      case 'O' =>
        val length = Varint.readUnsigned(b).intValue
        var dataObj = Record.NULL
        for (_ <- 0 until length) {
          val pId = new Array[Byte](varIntLength(b))
          b.get(pId)
          val pUriOpt = toUri(pId, EntryType.PropertyToId)
          val value = decode(b)
          pUriOpt.foreach { pUri => dataObj = dataObj.append(new Record(pUri, value)) }
        }
        dataObj
      // an array
      case '[' =>
        val length = Varint.readUnsigned(b).intValue
        val values = Array.ofDim[Any](length)
        for (i <- 0 until length) {
          values(i) = decode(b)
        }
        values
      // a URI reference
      case 'R' =>
        val refId = new Array[Byte](varIntLength(b))
        b.get(refId)
        toUri(refId, EntryType.ResourceToId)
          .getOrElse(URIs.createURI("urn:invalid:" + Varint.readUnsigned(ByteBuffer.wrap(refId))))
      // a scalar value
      case _ =>
        b.position(oldPos)
        Values.decode(b)
    }
  }

  def varIntLength(bytes: Array[Byte], pos: Int): Int = Varint.firstToLength(bytes(pos))

  def varIntLength(bb: ByteBuffer): Int = Varint.firstToLength(bb.get(bb.position()))

  def fetchInternal(item: URI, property: URI, context: URI, end: Long = KvinTuple.TIME_MAX_VALUE,
                    begin: Long = 0L, limit: Long = 0L, interval: Long = 0L): IExtendedIterator[KvinTuple] = {
    val currentContext = if (context == null) Kvin.DEFAULT_CONTEXT else context
    val propertiesIt: IExtendedIterator[URI] = if (property == null) {
      properties(item, context)
    } else {
      WrappedIterator.create(List(property).asJava.iterator())
    }

    if (!propertiesIt.hasNext()) NiceIterator.emptyIterator[KvinTuple] else {
      val it = values.iterator
      new StoreIterator[KvinTuple](it) {
        var currentProperty: URI = null
        var id: Array[Byte] = null
        val idTimePrefix: Array[Byte] = new Array[Byte](Varint.MAX_BYTES * 4)
        val prefixBuffer: ByteBuffer = ByteBuffer.wrap(idTimePrefix).order(BYTE_ORDER)

        var intervalSeq: Int = 0
        var count: Long = 0

        def nextProperty(): Unit = {
          var validProperty = false
          while (!validProperty && propertiesIt.hasNext) {
            currentProperty = propertiesIt.next()
            id = toId(item, currentProperty, context, generate = false)
            if (id != null) {
              validProperty = true
              util.Arrays.fill(idTimePrefix, 0.toByte)
              prefixBuffer.clear()
              prefixBuffer.put(id)
              writeVarint(prefixBuffer, end)
            }
          }
          if (validProperty) {
            count = 0
            it.seek(idTimePrefix)
          } else {
            close()
          }
        }

        override def init(): Unit = {
          nextProperty()
        }

        override def computeNext: Option[KvinTuple] = {
          val entry = it.next
          val key = entry.getKey
          if (key.startsWith(id)) {
            val bb = ByteBuffer.wrap(key, id.length, key.length - id.length).order(BYTE_ORDER)
            val time = readVarint(bb)
            val seq: Int = if (bb.hasRemaining) readVarint(bb).toInt else 0
            if (time <= end && time >= begin && (limit == 0 || count < limit)) {
              count += 1

              // skips time intervals if requested, the upper value is exclusive, the lower value is inclusive
              if (interval > 0) {
                val intervalStart = time - (time % interval)
                // seek to next interval
                (id.length until idTimePrefix.length).foreach(idTimePrefix(_) = 0)
                writeVarint(prefixBuffer, id.length, intervalStart - 1)
                it.seek(idTimePrefix)

                intervalSeq += 1
                Some(new KvinTuple(item, currentProperty, currentContext, intervalStart,
                  intervalSeq, decode(entry.getValue)))
              } else {
                Some(new KvinTuple(item, currentProperty, currentContext, time, seq,
                  decode(entry.getValue)))
              }
            } else {
              nextProperty()
              if (open) computeNext else None
            }
          } else {
            nextProperty()
            if (open) computeNext else None
          }
        }

        override def close(): Unit = {
          propertiesIt.close()
          super.close()
        }
      }
    }
  }

  override def close(): Unit = {
    var errors: List[IOException] = Nil
    try {
      ids.close()
    } catch {
      case e: IOException => errors ::= e
    }
    try {
      values.close()
    } catch {
      case e: IOException => errors ::= e
    }
    errors.headOption.foreach(e => throw new UncheckedIOException(e))
  }
}