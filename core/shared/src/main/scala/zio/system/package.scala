/*
 * Copyright 2017-2020 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio

import java.lang.{ System => JSystem }

import scala.collection.JavaConverters._

import com.github.ghik.silencer.silent

package object system {

  type System = Has[System.Service]

  object System extends Serializable {
    trait Service extends Serializable {
      def env(variable: String): IO[SecurityException, Option[String]]

      val envs: IO[SecurityException, Map[String, String]]

      val properties: IO[Throwable, Map[String, String]]

      def property(prop: String): IO[Throwable, Option[String]]

      def lineSeparator: UIO[String]
    }

    object Service {
      val live: Service = new Service {

        def env(variable: String): IO[SecurityException, Option[String]] =
          IO.effect(Option(JSystem.getenv(variable))).refineToOrDie[SecurityException]

        @silent("JavaConverters")
        val envs: IO[SecurityException, Map[String, String]] =
          IO.effect(JSystem.getenv.asScala.toMap).refineToOrDie[SecurityException]

        @silent("JavaConverters")
        val properties: IO[Throwable, Map[String, String]] =
          IO.effect(JSystem.getProperties.asScala.toMap)

        def property(prop: String): IO[Throwable, Option[String]] =
          IO.effect(Option(JSystem.getProperty(prop)))

        val lineSeparator: UIO[String] = IO.effectTotal(JSystem.lineSeparator)
      }
    }

    val any: ZLayer[System, Nothing, System] =
      ZLayer.requires[System]

    val live: Layer[Nothing, System] =
      ZLayer.succeed(Service.live)
  }

  /** Retrieve the value of an environment variable **/
  def env(variable: => String): ZIO[System, SecurityException, Option[String]] =
    ZIO.accessM(_.get env variable)

  /**
   * Retrieve the values of all environment variables.
   */
  val envs: ZIO[System, SecurityException, Map[String, String]] =
    ZIO.accessM(_.get.envs)

  /**
   * Retrieve the values of all system properties.
   */
  val properties: ZIO[System, Throwable, Map[String, String]] =
    ZIO.accessM(_.get.properties)

  /** Retrieve the value of a system property **/
  def property(prop: => String): ZIO[System, Throwable, Option[String]] =
    ZIO.accessM(_.get property prop)

  /** System-specific line separator **/
  val lineSeparator: ZIO[System, Nothing, String] =
    ZIO.accessM(_.get.lineSeparator)
}
