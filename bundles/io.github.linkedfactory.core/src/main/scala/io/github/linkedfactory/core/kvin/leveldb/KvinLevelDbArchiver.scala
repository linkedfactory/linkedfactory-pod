package io.github.linkedfactory.core.kvin.leveldb

import io.github.linkedfactory.core.kvin.KvinTuple
import io.github.linkedfactory.core.kvin.parquet.KvinParquet
import net.enilink.commons.iterator.IExtendedIterator

class KvinLevelDbArchiver(var databaseStore: KvinLevelDb, var archiveStore: KvinParquet) extends KvinLevelDbBase {
  def archive(): Unit = {
    val dbIterator: IExtendedIterator[KvinTuple] = databaseStore.fetchAll()
    try {
      archiveStore.put(dbIterator)
    } finally {
      dbIterator.close()
    }
  }
}
