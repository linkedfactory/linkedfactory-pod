package io.github.linkedfactory.kvin.archive

import io.github.linkedfactory.kvin.KvinTuple
import io.github.linkedfactory.kvin.parquet.KvinParquet
import io.github.linkedfactory.kvin.leveldb.Utils.RichBuffer
import io.github.linkedfactory.kvin.leveldb.{EntryType, KvinLevelDb, KvinLevelDbBase}
import net.enilink.commons.iterator.NiceIterator
import net.enilink.komma.core.{URI, URIs}
import org.iq80.leveldb.{DB, DBIterator, ReadOptions, Snapshot}

import java.nio.ByteBuffer
import java.util.Map.Entry

class DatabaseArchiver(var databaseStore: KvinLevelDb, var archiveStore: KvinParquet) extends KvinLevelDbBase {

  val ids: DB = databaseStore.getIdStore()
  val values: DB = databaseStore.getValueStore()

  private val idsSnapshot: Snapshot = ids.getSnapshot
  private val valuesSnapshot: Snapshot = values.getSnapshot

  private val idsSnapshotOption: ReadOptions = new ReadOptions
  private val valuesSnapshotOption: ReadOptions = new ReadOptions

  idsSnapshotOption.snapshot(idsSnapshot)
  valuesSnapshotOption.snapshot(valuesSnapshot)

  def getDatabaseIterator: NiceIterator[KvinTuple] = {
    new NiceIterator[KvinTuple] {
      // initial store iterators
      val idIterator: DBIterator = ids.iterator(idsSnapshotOption)
      val valueIterator: DBIterator = values.iterator(valuesSnapshotOption)
      idIterator.seek(Array(EntryType.SubjectToId.id.toByte))

      // terminating state variables
      var itemFinished = false
      var propertyFinished = false

      // general state variables
      var currentItemUri: URI = null
      var currentItemIdBytes: Array[Byte] = null
      var currentPropertyIterator: DBIterator = null
      var currentPropertyPrefix: Array[Byte] = null
      var nextItem: Entry[Array[Byte], Array[Byte]] = null
      var nextProperty: Entry[Array[Byte], Array[Byte]] = null
      var isReadingProperties = false
      var isEndOfCurrentItemProperties = false
      var isLastItem = false
      var isEndOfAllProperties = false

      var tuple: KvinTuple = null

      override def hasNext: Boolean = {
        if (itemFinished || (isLastItem && isEndOfAllProperties) || !idIterator.hasNext) {
          close()
          false
        } else {
          true
        }
      }

      override def next(): KvinTuple = {
        if (!itemFinished && !isReadingProperties) {
          // resetting property state on new item reading
          propertyFinished = false
          // reading items
          val entry = if (nextItem == null) idIterator.next() else nextItem
          val key: Array[Byte] = entry.getKey
          if (key(0) == EntryType.SubjectToId.id) {
            val uriBytes = new Array[Byte](key.indexOf(0) - 1)
            System.arraycopy(key, 1, uriBytes, 0, uriBytes.length)
            val uri = URIs.createURI(new String(uriBytes, "UTF-8"))
            val idBytes = entry.getValue
            currentItemUri = uri
            currentItemIdBytes = idBytes

            // checks if next item is the last item.
            // one of the condition for indicating end of all records in the leveldb store.
            nextItem = idIterator.next()
            val nextItemKey = nextItem.getKey
            if (nextItemKey(0) != EntryType.SubjectToId.id) isLastItem = true
          } else {
            itemFinished = true
          }
        }

        // only get new id iterator and prefix if all of the properties of the current item are read.
        if (!isReadingProperties || currentPropertyIterator == null) {
          currentPropertyIterator = ids.iterator(idsSnapshotOption)
          val prefix = new Array[Byte](currentItemIdBytes.length + 1)
          prefix(0) = EntryType.SPCtoId.id.toByte
          System.arraycopy(currentItemIdBytes, 0, prefix, 1, currentItemIdBytes.length)
          currentPropertyPrefix = prefix
          currentPropertyIterator.seek(prefix)
        }

        // reading properties and context of current item.
        if (!propertyFinished) {
          isReadingProperties = true
          // reading property and context of current item
          if (!isEndOfCurrentItemProperties) {
            val entry = if (nextProperty == null) currentPropertyIterator.next else nextProperty
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

              // reading time, seqNr & value of current property
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

            nextProperty = currentPropertyIterator.next()
            val nextPropertyKey = nextProperty.getKey
            // true when all properties are read for the current item
            if (!nextPropertyKey.startsWith(currentPropertyPrefix)) isEndOfCurrentItemProperties = true
            // true when all the properties of the last item are read.
            // one of the condition for indicating end of all records in the leveldb store.
            if (!nextPropertyKey.startsWith(currentPropertyPrefix) && isLastItem) isEndOfAllProperties = true
          } else {
            propertyFinished = true
            isReadingProperties = false
            isEndOfCurrentItemProperties = false
            nextProperty = null
            next() // recursive call to next if all properties of the current item are read.
          }
        }
        tuple
      }

      override def close(): Unit = {
        idIterator.close()
        valueIterator.close()
        idsSnapshot.close()
        valuesSnapshot.close()
        if (currentPropertyIterator != null) currentPropertyIterator.close()
      }
    }
  }

  def archive(): Unit = {
    val dbIterator: NiceIterator[KvinTuple] = getDatabaseIterator
    archiveStore.put(dbIterator)
    dbIterator.close()
  }
}

