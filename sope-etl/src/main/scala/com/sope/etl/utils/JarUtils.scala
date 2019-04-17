package com.sope.etl.utils

import java.io._
import java.util.jar.{JarEntry, JarOutputStream}

/**
  * Jar Utilities
  *
  * @author mbadgujar
  */
object JarUtils {


  /**
    * Builds jar from content of provided folder
    *
    * @param classFolder Class Folder
    * @param jarLocation full jar path along with jar file name
    */
  def buildJar(classFolder: String, jarLocation: String): Unit = {
    val target = new JarOutputStream(new FileOutputStream(jarLocation))
    add(classFolder, target, classFolder)
    target.close()
  }

  // adds files to jar
  private def add(folder: String, target: JarOutputStream, replacement: String): Unit = {
    var in: BufferedInputStream = null
    try {
      val source = new File(folder)
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
          add(nestedFile.getAbsolutePath, target, replacement)
        }
        return
      }

      val entry = new JarEntry(source.getPath
        .replace("\\", "/")
        .replace(replacement, ""))
      entry.setTime(source.lastModified)
      target.putNextEntry(entry)
      in = new BufferedInputStream(new FileInputStream(source))
      val buffer = new Array[Byte](1024)
      var count: Int = 0
      while (count != -1) {
        count = in.read(buffer)
        if (count != -1)
          target.write(buffer, 0, count)
      }
      target.closeEntry()
    }
    finally {
      if (in != null) in.close()
    }
  }
}