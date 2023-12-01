package io.github.linkedfactory.core.kvin

import io.github.linkedfactory.core.kvin.leveldb.{KvinLevelDb, KvinLevelDbArchiver}
import io.github.linkedfactory.core.kvin.parquet.KvinParquet
import io.github.linkedfactory.core.kvin.util.KvinTupleGenerator
import net.enilink.commons.iterator.NiceIterator
import org.apache.commons.io.FileUtils
import org.junit.Assert.assertTrue
import org.junit.{After, Before, Test}

import java.io.File
import java.nio.file.Files

class KvinLevelDbArchiverTest {

  var archiveTempDir: File = null
  var leveldbTempDir: File = null
  var databaseStore: KvinLevelDb = null
  var archiveStore: KvinParquet = null

  @Before
  def setup(): Unit = {
    archiveTempDir = Files.createTempDirectory("archive").toFile
    leveldbTempDir = Files.createTempDirectory("level0db-test").toFile
    databaseStore = new KvinLevelDb(leveldbTempDir)
    archiveStore = new KvinParquet(archiveTempDir.getAbsolutePath + "/")
    addTestData()
  }

  @After
  def cleanup(): Unit = {
    FileUtils.deleteDirectory(new File(archiveTempDir.getPath))
    FileUtils.deleteDirectory(new File(leveldbTempDir.getPath))
  }

  def addTestData(): Unit = {
    val data: NiceIterator[KvinTuple] = new KvinTupleGenerator().generate(1697407200000L, 10, 10, 10,
      "http://localhost:8080/linkedfactory/demofactory/new-week/{}",
      "http://example.org/{}");
    while (data.hasNext) {
      databaseStore.put(data.next)
    }
  }

  @Test
  def testLevelDbToParquetArchival(): Unit = {
    val dbArchiver: KvinLevelDbArchiver = new KvinLevelDbArchiver(databaseStore, archiveStore)
    dbArchiver.archive()
    assertTrue(new File(archiveTempDir.getPath).listFiles.length > 0)
  }
}
