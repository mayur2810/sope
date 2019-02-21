package com.sope.etl.utils

import java.io._
import java.util.jar.{JarEntry, JarOutputStream}

import com.sope.etl.register.UDFBuilder

/**
  *
  * @author mbadgujar
  */
object CreateJar {

  def build(tempDir: String, jarLocation: String): Unit = {
    val target = new JarOutputStream(new FileOutputStream(jarLocation))
    add(new File(tempDir), target)
    target.close()
  }


  private def add(source: File, target: JarOutputStream): Unit = {
    var in: BufferedInputStream = null
    try {
      if (source.isDirectory) {
        var name = source.getPath.replace("\\", "/")
        if (!name.isEmpty) {
          if (!name.endsWith("/")) name += "/"
          val entry = new JarEntry(name)
          entry.setTime(source.lastModified)
          target.putNextEntry(entry)
          target.closeEntry()
        }

        for (nestedFile <- source.listFiles) {
          add(nestedFile, target)
        }
        return
      }
      println(source)
      val entry = new JarEntry(source.getPath.replace("\\", "/").replace("/tmp/sope/dynamic/", ""))
      entry.setTime(source.lastModified)
      target.putNextEntry(entry)
      println(entry)
      in = new BufferedInputStream(new FileInputStream(source))
      val buffer = new Array[Byte](1024)
      var count: Int = 0
      while (count != -1) {
        count = in.read(buffer)
        if (count != -1)
          target.write(buffer, 0, count)
      }
      //val count = in.read(buffer)
      target.closeEntry()
    }
    finally {
      if (in != null) in.close()
    }
  }
}
