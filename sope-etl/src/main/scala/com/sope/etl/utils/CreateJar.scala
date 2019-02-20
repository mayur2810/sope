package com.sope.etl.utils

import java.io._
import java.util.jar.{JarEntry, JarOutputStream}

/**
  *
  * @author mbadgujar
  */
object CreateJar {

  @throws[IOException]
  def run(tempDir: String): Unit = {
    //val manifest = new Manifest
    //manifest.getMainAttributes.put(Attributes.Name.MANIFEST_VERSION, "1.0")
    val target = new JarOutputStream(new FileOutputStream(s"/tmp/sope/sope-dynamic-udf.jar"))
    add(new File(tempDir), target)
    target.close()
  }

  @throws[IOException]
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
      val count = in.read(buffer)
      target.write(buffer, 0, count)
      target.closeEntry()
    }
    finally {
      if (in != null) in.close()
    }
  }

  def main(args: Array[String]): Unit = {

  }
}
