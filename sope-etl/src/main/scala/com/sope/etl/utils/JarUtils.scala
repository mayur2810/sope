package com.sope.etl.utils

import java.io._
import java.nio.file.Files
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
    val source = new File(folder)
    if (source.isDirectory) {
      val name = source.getPath.replace("\\", "/")
      if (!name.isEmpty) {
        val folderName = if (!name.endsWith("/")) name + "/" else name
        val entry = new JarEntry(folderName)
        entry.setTime(source.lastModified)
        target.putNextEntry(entry)
        target.closeEntry()
      }
      for (nestedFile <- source.listFiles) {
        add(nestedFile.getAbsolutePath, target, replacement)
      }
    } else {
      val entry = new JarEntry(source.getPath
        .replace("\\", "/")
        .replace(replacement, ""))
      entry.setTime(source.lastModified)
      target.putNextEntry(entry)
      val byteArray = Files.readAllBytes(source.toPath)
      target.write(byteArray)
      target.closeEntry()
    }
  }
}