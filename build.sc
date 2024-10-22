import $ivy.`com.lihaoyi::mill-contrib-scoverage:`
import mill.contrib.scoverage.ScoverageModule
import $ivy.`com.goyeau::mill-git::0.2.5`
import com.goyeau.mill.git.GitVersionedPublishModule
import mill._
import mill.scalalib._
import mill.scalalib.publish._

trait SopeModule extends GitVersionedPublishModule with SbtModule with ScoverageModule {
  override def scalaVersion = "2.12.18"
  def sparkVersion = "3.1.3"
  def scalaTestVersion = "3.2.16"
  def jacksonYamlVersion = "2.6.5"
  def bigqueryConnectorVersion = "hadoop3-0.13.11"
  def scoptVersion = "3.7.1"
  def scoverageVersion = "2.0.10"

  def pomSettings = PomSettings(
    organization = "com.mayurb",
    description = "Sope",
    url = "https://github.com/mayur2810/sope",
    licenses = Seq(License.`Apache-2.0`),
    versionControl = VersionControl.github("mayur2810", "sope"),
    developers = Seq(Developer(id = "mayur2810", name = "Mayur Badgujar", url = "https://github.com/mayur2810"))
  )
}
object `sope-spark` extends SopeModule {
  def ivyDeps = Agg(
    ivy"org.apache.spark::spark-core:${sparkVersion}",
    ivy"org.apache.spark::spark-sql:${sparkVersion}",
    ivy"org.apache.spark::spark-hive:${sparkVersion}",
    ivy"com.github.scopt::scopt:${scoptVersion}",
    ivy"com.google.cloud.bigdataoss:bigquery-connector:${bigqueryConnectorVersion}"
  )
  object test extends SbtModuleTests with TestModule.ScalaTest with ScoverageTests {
    def ivyDeps = Agg(ivy"org.scalatest::scalatest::${scalaTestVersion}")
  }
}

object `sope-etl` extends SopeModule {
  override def moduleDeps = Seq(`sope-spark`)
  def ivyDeps = Agg(
    ivy"org.scala-lang:scala-compiler:${scalaVersion}",
    ivy"com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:${jacksonYamlVersion}",
    ivy"com.github.scopt::scopt:${scoptVersion}",
    ivy"com.google.cloud.bigdataoss:bigquery-connector:${bigqueryConnectorVersion}"
  )

  object test extends SbtModuleTests with TestModule.ScalaTest {
    def resources = T { super.resources() ++ Seq(PathRef(millSourcePath / "templates")) }
    def ivyDeps = Agg(ivy"org.scalatest::scalatest::${scalaTestVersion}")
  }

  def millModuleDeps = Seq(`sope-spark`, `sope-etl`)
}
