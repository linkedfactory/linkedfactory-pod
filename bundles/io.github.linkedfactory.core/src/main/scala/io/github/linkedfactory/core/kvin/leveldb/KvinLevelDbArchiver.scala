package io.github.linkedfactory.core.kvin.leveldb

import io.github.linkedfactory.core.kvin.KvinTuple
import io.github.linkedfactory.core.kvin.parquet.KvinParquet
import net.enilink.commons.iterator.{IExtendedIterator, NiceIterator}
import net.enilink.komma.core.{URI, URIs}
import org.iq80.leveldb.{DB, DBIterator, ReadOptions, Snapshot}

import java.util.Map.Entry

class KvinLevelDbArchiver(var databaseStore: KvinLevelDb, var archiveStore: KvinParquet) extends KvinLevelDbBase {

  val ids: DB = databaseStore.getIdStore()
  val EntryType = databaseStore.getEntryTypeObj();

  private val idsSnapshot: Snapshot = ids.getSnapshot
  private val idsSnapshotOption: ReadOptions = new ReadOptions

  idsSnapshotOption.snapshot(idsSnapshot)

  def getDatabaseIterator: IExtendedIterator[KvinTuple] = {
    new NiceIterator[KvinTuple] {
      // initial store iterators
      val idIterator: DBIterator = ids.iterator(idsSnapshotOption)
      idIterator.seek(Array(EntryType.SubjectToId.id.toByte))

      //state variables
      var isLoopingProperties = false
      var propertyIterator: IExtendedIterator[KvinTuple] = null
      var tuple: KvinTuple = null
      var currentItem: URI = null
      var nextItem: Entry[Array[Byte], Array[Byte]] = null
      var isLastItem = false
      var isEndOfAllRecords = !idIterator.hasNext()

      override def hasNext: Boolean = {
        !isEndOfAllRecords
      }

      override def next(): KvinTuple = {
        // reading items
        if (!isLoopingProperties) {
          val entry = if (nextItem == null) idIterator.next() else nextItem
          val key: Array[Byte] = entry.getKey
          if (key(0) == EntryType.SubjectToId.id) {
            val uriBytes = new Array[Byte](key.indexOf(0) - 1)
            System.arraycopy(key, 1, uriBytes, 0, uriBytes.length)
            val item = URIs.createURI(new String(uriBytes, "UTF-8"))
            currentItem = item

            // checking if next item is the last item.
            nextItem = idIterator.next()
            val nextItemKey = nextItem.getKey
            if (nextItemKey(0) != EntryType.SubjectToId.id) isLastItem = true
          }
        }

        // reading properties
        if (propertyIterator == null) {
          propertyIterator = databaseStore.fetch(currentItem, null, URIs.createURI("kvin:nil"), 0)
        }
        if (propertyIterator.hasNext) {
          isLoopingProperties = true
          tuple = propertyIterator.next()
          // marking end of all records if there is no more properties left of the last item
          if (isLastItem && !propertyIterator.hasNext) isEndOfAllRecords = true
        } else {
          endPropertyLooping()
        }
        tuple
      }

      def endPropertyLooping(): Unit = {
        isLoopingProperties = false
        propertyIterator.close()
        propertyIterator = null
        currentItem = null
        tuple = null
        next()
      }

      override def close(): Unit = {
        if (propertyIterator != null) {
          propertyIterator.close();
        }
        idIterator.close()
        idsSnapshot.close()
      }
    }
  }

  def archive(): Unit = {
    val dbIterator: IExtendedIterator[KvinTuple] = getDatabaseIterator
    try {
      archiveStore.put(dbIterator)
    } finally {
      dbIterator.close()
    }
  }
}
