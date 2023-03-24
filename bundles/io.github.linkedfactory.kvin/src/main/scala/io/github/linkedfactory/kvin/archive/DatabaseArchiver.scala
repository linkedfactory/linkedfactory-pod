package io.github.linkedfactory.kvin.archive

import io.github.linkedfactory.kvin.KvinTuple
import io.github.linkedfactory.kvin.kvinparquet.KvinParquet
import io.github.linkedfactory.kvin.leveldb.Utils.RichBuffer
import io.github.linkedfactory.kvin.leveldb.{EntryType, KvinLevelDb, KvinLevelDbBase}
import net.enilink.commons.iterator.NiceIterator
import net.enilink.komma.core.{URI, URIs}

import org.iq80.leveldb.{DB, DBIterator}

import java.nio.ByteBuffer

class DatabaseArchiver(var databaseStore: KvinLevelDb, var archiveStore: KvinParquet) extends KvinLevelDbBase {

  val ids: DB = databaseStore.getIdStore()
  val values: DB = databaseStore.getValueStore()

  private def getDatabaseIterator: NiceIterator[KvinTuple] = {
    new NiceIterator[KvinTuple] {
      val idIterator: DBIterator = ids.iterator()
      val valueIterator: DBIterator = values.iterator()
      idIterator.seek(Array(EntryType.SubjectToId.id.toByte))

      var itemFinished = false
      var propertyFinished = false

      var isReadingProperties = false

      var currentItemUri: URI = null
      var currentItemIdBytes: Array[Byte] = null
      var currentPropertyIterator: DBIterator = null
      var currentPropertyPrefix: Array[Byte] = null

      var tuple: KvinTuple = null

      override def hasNext: Boolean = {
        if (itemFinished == true) {
          close()
        }
        !itemFinished
      }

      override def next(): KvinTuple = {
        // reading items
        if (idIterator.hasNext && !itemFinished && !isReadingProperties) {
          propertyFinished = false
          val entry = idIterator.next()
          val key: Array[Byte] = entry.getKey
          if (key(0) == EntryType.SubjectToId.id) {
            val uriBytes = new Array[Byte](key.indexOf(0) - 1)
            System.arraycopy(key, 1, uriBytes, 0, uriBytes.length)
            val uri = URIs.createURI(new String(uriBytes, "UTF-8"))
            val idBytes = entry.getValue
            currentItemUri = uri
            currentItemIdBytes = idBytes
          } else {
            itemFinished = true
          }
        }

        // reading properties & context
        if (!isReadingProperties || currentPropertyIterator == null) {
          currentPropertyIterator = ids.iterator()
          val prefix = new Array[Byte](currentItemIdBytes.length + 1)
          prefix(0) = EntryType.SPCtoId.id.toByte
          System.arraycopy(currentItemIdBytes, 0, prefix, 1, currentItemIdBytes.length)
          currentPropertyPrefix = prefix
          currentPropertyIterator.seek(prefix)
        }

        if (currentPropertyIterator.hasNext && !propertyFinished) {
          isReadingProperties = true
          val entry = currentPropertyIterator.next
          val key = entry.getKey
          if (key.startsWith(currentPropertyPrefix)) {
            val pos = currentPropertyPrefix.length
            val propertyIdLength = databaseStore.varIntLength(key, pos)
            val contextIdLength = databaseStore.varIntLength(key, pos + 1)
            val propertyIdBytes = new Array[Byte](propertyIdLength)
            val contextIdBytes = new Array[Byte](contextIdLength)
            System.arraycopy(key, pos, propertyIdBytes, 0, propertyIdBytes.length)
            System.arraycopy(key, pos + 1, contextIdBytes, 0, contextIdBytes.length)
            val property: Option[URI] = databaseStore.toUri(propertyIdBytes, EntryType.PropertyToId)
            val context: Option[URI] = databaseStore.toUri(contextIdBytes, EntryType.ContextToId)

            // reading time, seqNr & value
            val valuePrefix = entry.getValue
            valueIterator.seek(valuePrefix)

            if (valueIterator.hasNext) {
              val entry = valueIterator.next
              val key = entry.getKey
              if (key.startsWith(valuePrefix)) {
                val bb = ByteBuffer.wrap(key, valuePrefix.length, key.length - valuePrefix.length).order(BYTE_ORDER)
                val time = mapTime(bb.getInt6)
                val seqNr = if (bb.hasRemaining) mapSeq(bb.getShortUnsigned) else 0
                val value = databaseStore.decode(entry.getValue)
                tuple = new KvinTuple(currentItemUri, property.get, context.get, time, seqNr, value)
              }
            }
          }
          else {
            propertyFinished = true
            isReadingProperties = false
            next()
          }
        }
        tuple
      }

      override def close(): Unit = {
        idIterator.close()
        valueIterator.close()
        currentPropertyIterator.close()
      }
    }
  }

  def archive(): Unit = {
    val dbIterator: NiceIterator[KvinTuple] = getDatabaseIterator
    archiveStore.put(dbIterator)
    dbIterator.close()
  }
}

