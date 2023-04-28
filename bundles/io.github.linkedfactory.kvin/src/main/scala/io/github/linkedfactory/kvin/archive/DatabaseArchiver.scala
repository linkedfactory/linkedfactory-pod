package io.github.linkedfactory.kvin.archive

import io.github.linkedfactory.kvin.KvinTuple
import io.github.linkedfactory.kvin.parquet.KvinParquet
import io.github.linkedfactory.kvin.leveldb.Utils.RichBuffer
import io.github.linkedfactory.kvin.leveldb.{KvinLevelDb, KvinLevelDbBase}
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

      override def hasNext: Boolean = {
        false
      }

      override def next(): KvinTuple = {
        null
      }
    }
  }

  def archive(): Unit = {
    val dbIterator: NiceIterator[KvinTuple] = getDatabaseIterator
    archiveStore.put(dbIterator)
    dbIterator.close()
  }
}

