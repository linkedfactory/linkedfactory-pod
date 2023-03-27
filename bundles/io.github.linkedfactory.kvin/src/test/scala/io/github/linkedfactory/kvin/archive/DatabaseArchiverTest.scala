package io.github.linkedfactory.kvin.archive

import io.github.linkedfactory.kvin.KvinTuple
import io.github.linkedfactory.kvin.parquet.{KvinParquet, KvinParquetTestBase}
import io.github.linkedfactory.kvin.leveldb.KvinLevelDb
import net.enilink.commons.iterator.NiceIterator
import org.apache.commons.io.FileUtils
import org.junit.Assert.assertTrue
import org.junit.{After, Before, Test}

import java.io.File
import java.nio.file.Files

class DatabaseArchiverTest {

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
    val data: NiceIterator[KvinTuple] = KvinParquetTestBase.generateRandomKvinTuples(50, 500, 10)
    while (data.hasNext) {
      databaseStore.put(data.next)
    }
  }

  @Test
  def testLevelDbToParquetArchival(): Unit = {
    val dbArchiver: DatabaseArchiver = new DatabaseArchiver(databaseStore, archiveStore)
    dbArchiver.archive()
    assertTrue(new File(archiveTempDir.getPath).listFiles.length > 0)
  }


}
