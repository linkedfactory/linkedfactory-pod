package io.github.linkedfactory.service

import java.io.{ByteArrayInputStream, InputStream}
import javax.servlet.ServletInputStream

/**
 * Lift's version is not fully working. Therefore some modifications are required.
 *
 * @param url The target URL
 */
class MockHttpServletRequest(url: String) extends net.liftweb.mocks.MockHttpServletRequest(url) {
  class MockServletInputStream(is: InputStream) extends ServletInputStream {
    def read(): Int = is.read()

    override def available(): Int = is.available()

    def isFinished(): Boolean = is.available() == 0

    def isReady(): Boolean = !isFinished()

    def setReadListener(l: javax.servlet.ReadListener): Unit = ()
  }

  override def getInputStream(): ServletInputStream = {
    new MockServletInputStream(new ByteArrayInputStream(body))
  }
}